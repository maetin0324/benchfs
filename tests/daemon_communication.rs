//! Integration tests for client-daemon communication via shared memory.
//!
//! These tests verify the correctness of:
//! - Shared memory creation and attachment
//! - Ring buffer SPSC operations
//! - Slot allocation and deallocation
//! - Request/response round-trip communication

#![cfg(feature = "daemon-mode")]

use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use benchfs::daemon::protocol::{OpType, RequestEntry, ResponseEntry, INLINE_PATH_LEN};
use benchfs::daemon::ring::{RequestRing, ResponseRing};
use benchfs::daemon::shm::{SharedMemoryRegion, ShmConfig};

/// Generate a unique shared memory name for each test
fn unique_shm_name(test_name: &str) -> String {
    format!(
        "benchfs_test_{}_{}_{}",
        test_name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Clean up shared memory after test
fn cleanup_shm(name: &str) {
    let shm_path = format!("/{}", name);
    unsafe {
        let c_name = std::ffi::CString::new(shm_path).unwrap();
        libc::shm_unlink(c_name.as_ptr());
    }
}

// ============================================================================
// Shared Memory Tests
// ============================================================================

#[test]
fn test_shm_create_and_attach() {
    let shm_name = unique_shm_name("create_attach");

    // Create shared memory (daemon side)
    let config = ShmConfig {
        num_slots: 4,
        data_buffer_size: 4096,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let creator = SharedMemoryRegion::create(&shm_name, config.clone()).unwrap();

    // Verify initial state
    assert!(!creator.is_daemon_ready());
    assert!(!creator.is_shutdown_requested());

    // Set daemon ready
    creator.set_daemon_ready(12345);
    assert!(creator.is_daemon_ready());

    // Attach from client side
    let client = SharedMemoryRegion::attach(&shm_name).unwrap();
    assert!(client.is_daemon_ready());

    // Verify config matches
    assert_eq!(client.config().num_slots, 4);
    assert_eq!(client.config().data_buffer_size, 4096);

    cleanup_shm(&shm_name);
}

#[test]
fn test_slot_allocation() {
    let shm_name = unique_shm_name("slot_alloc");

    let config = ShmConfig {
        num_slots: 4,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);

    // Allocate all slots
    let slot0 = shm.allocate_slot().unwrap();
    let slot1 = shm.allocate_slot().unwrap();
    let slot2 = shm.allocate_slot().unwrap();
    let slot3 = shm.allocate_slot().unwrap();

    assert_eq!(slot0, 0);
    assert_eq!(slot1, 1);
    assert_eq!(slot2, 2);
    assert_eq!(slot3, 3);

    // No more slots available
    assert!(shm.allocate_slot().is_err());

    // Release a slot
    shm.release_slot(1).unwrap();

    // Can allocate again
    let slot_reused = shm.allocate_slot().unwrap();
    assert_eq!(slot_reused, 1);

    cleanup_shm(&shm_name);
}

#[test]
fn test_slot_control_block() {
    let shm_name = unique_shm_name("slot_control");

    let config = ShmConfig {
        num_slots: 2,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);

    let slot_id = shm.allocate_slot().unwrap();
    let slot = shm.slot_control(slot_id).unwrap();

    // Set client PID
    slot.client_pid.store(42, Ordering::Release);
    assert_eq!(slot.client_pid.load(Ordering::Acquire), 42);

    // Verify ring indices are initially zero
    assert_eq!(slot.request_head.load(Ordering::Acquire), 0);
    assert_eq!(slot.request_tail.load(Ordering::Acquire), 0);
    assert_eq!(slot.response_head.load(Ordering::Acquire), 0);
    assert_eq!(slot.response_tail.load(Ordering::Acquire), 0);

    cleanup_shm(&shm_name);
}

#[test]
fn test_data_buffer_access() {
    let shm_name = unique_shm_name("data_buffer");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 4096,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);

    let slot_id = shm.allocate_slot().unwrap();

    // Write to data buffer
    {
        let buffer = shm.data_buffer_mut(slot_id).unwrap();
        assert_eq!(buffer.len(), 4096);
        buffer[0..4].copy_from_slice(b"test");
    }

    // Read back
    {
        let buffer = shm.data_buffer(slot_id).unwrap();
        assert_eq!(&buffer[0..4], b"test");
    }

    cleanup_shm(&shm_name);
}

// ============================================================================
// Ring Buffer Tests
// ============================================================================

#[test]
fn test_request_ring_push_pop() {
    let shm_name = unique_shm_name("req_ring");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 4, // Small ring for testing wraparound
        response_ring_size: 4,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);
    let slot_id = shm.allocate_slot().unwrap();

    let ring = RequestRing::new(&shm, slot_id).unwrap();

    // Initially empty
    assert!(ring.try_pop().is_none());

    // Push a request
    let mut req1 = RequestEntry::new();
    req1.request_id = 1;
    req1.op_type = OpType::Open as u32;
    req1.set_inline_path("/test/file1");
    assert!(ring.push(&req1));

    // Pop and verify
    let popped = ring.try_pop().unwrap();
    assert_eq!(popped.request_id, 1);
    assert_eq!(popped.op(), OpType::Open);
    assert_eq!(popped.get_inline_path(), Some("/test/file1"));

    // Empty again
    assert!(ring.try_pop().is_none());

    cleanup_shm(&shm_name);
}

#[test]
fn test_response_ring_push_pop() {
    let shm_name = unique_shm_name("resp_ring");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 4,
        response_ring_size: 4,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);
    let slot_id = shm.allocate_slot().unwrap();

    let ring = ResponseRing::new(&shm, slot_id).unwrap();

    // Push success response
    let resp1 = ResponseEntry::success(100, 42);
    assert!(ring.push(&resp1));

    // Push error response
    let resp2 = ResponseEntry::error(101, -5);
    assert!(ring.push(&resp2));

    // try_pop_by_id only matches the head (next to pop) entry
    // So we need to pop in order, not out of order
    // First pop resp1 (request_id=100) since it was pushed first
    let popped1 = ring.try_pop_by_id(100).unwrap();
    assert_eq!(popped1.request_id, 100);
    assert_eq!(popped1.result, 42);
    assert!(popped1.is_success());

    // Now pop resp2 (request_id=101)
    let popped2 = ring.try_pop_by_id(101).unwrap();
    assert_eq!(popped2.request_id, 101);
    assert_eq!(popped2.status, -5);
    assert!(!popped2.is_success());

    cleanup_shm(&shm_name);
}

#[test]
fn test_ring_wraparound() {
    let shm_name = unique_shm_name("wraparound");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 4,
        response_ring_size: 4,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);
    let slot_id = shm.allocate_slot().unwrap();

    let ring = RequestRing::new(&shm, slot_id).unwrap();

    // Fill and empty the ring multiple times to test wraparound
    for iteration in 0..3 {
        // Fill ring (capacity entries - ring can hold all 4 entries)
        for i in 0..4 {
            let mut req = RequestEntry::new();
            req.request_id = (iteration * 10 + i) as u64;
            assert!(ring.push(&req), "Failed to push at iteration {}, i {}", iteration, i);
        }

        // Ring should be full
        let mut full_req = RequestEntry::new();
        full_req.request_id = 999;
        assert!(!ring.push(&full_req), "Should not push to full ring");

        // Empty ring
        for i in 0..4 {
            let popped = ring.try_pop().expect("Should pop");
            assert_eq!(popped.request_id, (iteration * 10 + i) as u64);
        }

        // Ring should be empty
        assert!(ring.try_pop().is_none());
    }

    cleanup_shm(&shm_name);
}

#[test]
fn test_ring_full_detection() {
    let shm_name = unique_shm_name("ring_full");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 4,
        response_ring_size: 4,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);
    let slot_id = shm.allocate_slot().unwrap();

    let ring = RequestRing::new(&shm, slot_id).unwrap();

    // Ring size is 4, so we can push exactly 4 entries
    for i in 0..4 {
        let mut req = RequestEntry::new();
        req.request_id = i;
        assert!(ring.push(&req));
    }

    // 5th push should fail (ring is full)
    let mut req = RequestEntry::new();
    req.request_id = 999;
    assert!(!ring.push(&req));

    cleanup_shm(&shm_name);
}

// ============================================================================
// Cross-thread Communication Tests (simulating client-daemon)
// ============================================================================

#[test]
fn test_client_daemon_request_response() {
    let shm_name = unique_shm_name("client_daemon");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 4096,
        request_ring_size: 16,
        response_ring_size: 16,
    };

    // Create shared memory (daemon side)
    let daemon_shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    daemon_shm.set_daemon_ready(std::process::id());

    let shm_name_clone = shm_name.clone();

    // Spawn "daemon" thread
    let daemon_handle = thread::spawn(move || {
        let shm = SharedMemoryRegion::attach(&shm_name_clone).unwrap();

        // Wait for slot allocation
        loop {
            if let Ok(slot) = shm.slot_control(0) {
                if slot.client_pid.load(Ordering::Acquire) != 0 {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }

        let req_ring = RequestRing::new(&shm, 0).unwrap();
        let resp_ring = ResponseRing::new(&shm, 0).unwrap();

        // Process 5 requests
        let mut processed = 0;
        while processed < 5 {
            if let Some(req) = req_ring.try_pop() {
                // Simulate processing: return request_id * 2
                let result = req.request_id * 2;
                let resp = ResponseEntry::success(req.request_id, result);
                while !resp_ring.push(&resp) {
                    thread::yield_now();
                }
                processed += 1;
            } else {
                thread::yield_now();
            }
        }
    });

    // Client side
    let client_shm = SharedMemoryRegion::attach(&shm_name).unwrap();
    let slot_id = client_shm.allocate_slot().unwrap();

    // Set client PID
    let slot = client_shm.slot_control(slot_id).unwrap();
    slot.client_pid.store(std::process::id(), Ordering::Release);

    let req_ring = RequestRing::new(&client_shm, slot_id).unwrap();
    let resp_ring = ResponseRing::new(&client_shm, slot_id).unwrap();

    // Send 5 requests and verify responses
    for i in 1..=5 {
        let mut req = RequestEntry::new();
        req.request_id = i;
        req.op_type = OpType::Read as u32;

        while !req_ring.push(&req) {
            thread::yield_now();
        }

        // Wait for response
        let resp = loop {
            if let Some(r) = resp_ring.try_pop_by_id(i) {
                break r;
            }
            thread::yield_now();
        };

        assert!(resp.is_success());
        assert_eq!(resp.request_id, i);
        assert_eq!(resp.result, i * 2);
    }

    daemon_handle.join().unwrap();
    cleanup_shm(&shm_name);
}

#[test]
fn test_data_transfer_via_shared_buffer() {
    let shm_name = unique_shm_name("data_transfer");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let daemon_shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    daemon_shm.set_daemon_ready(1);

    let shm_name_clone = shm_name.clone();

    // Daemon thread: reads data from buffer and echoes back with uppercase
    let daemon_handle = thread::spawn(move || {
        let shm = SharedMemoryRegion::attach(&shm_name_clone).unwrap();

        // Wait for slot allocation
        loop {
            if let Ok(slot) = shm.slot_control(0) {
                if slot.client_pid.load(Ordering::Acquire) != 0 {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }

        let req_ring = RequestRing::new(&shm, 0).unwrap();
        let resp_ring = ResponseRing::new(&shm, 0).unwrap();

        // Process one write request
        loop {
            if let Some(req) = req_ring.try_pop() {
                if req.op() == OpType::Write {
                    let data_offset = req.data_offset as usize;
                    let data_len = req.length as usize;

                    // Read data from client's buffer
                    let buffer = shm.data_buffer(0).unwrap();
                    let data = &buffer[data_offset..data_offset + data_len];

                    // Convert to uppercase and write back
                    let upper: Vec<u8> = data.iter().map(|b| b.to_ascii_uppercase()).collect();

                    let buffer_mut = shm.data_buffer_mut(0).unwrap();
                    buffer_mut[0..upper.len()].copy_from_slice(&upper);

                    let resp = ResponseEntry::success_with_data(
                        req.request_id,
                        upper.len() as u64,
                        0,
                        upper.len() as u64,
                    );
                    while !resp_ring.push(&resp) {
                        thread::yield_now();
                    }
                    break;
                }
            }
            thread::yield_now();
        }
    });

    // Client side
    let client_shm = SharedMemoryRegion::attach(&shm_name).unwrap();
    let slot_id = client_shm.allocate_slot().unwrap();
    let slot = client_shm.slot_control(slot_id).unwrap();
    slot.client_pid.store(std::process::id(), Ordering::Release);

    // Write data to buffer
    let test_data = b"hello world";
    {
        let buffer = client_shm.data_buffer_mut(slot_id).unwrap();
        buffer[0..test_data.len()].copy_from_slice(test_data);
    }

    // Send write request
    let req_ring = RequestRing::new(&client_shm, slot_id).unwrap();
    let resp_ring = ResponseRing::new(&client_shm, slot_id).unwrap();

    let mut req = RequestEntry::new();
    req.request_id = 1;
    req.op_type = OpType::Write as u32;
    req.data_offset = 0;
    req.length = test_data.len() as u64;
    assert!(req_ring.push(&req));

    // Wait for response
    let resp = loop {
        if let Some(r) = resp_ring.try_pop_by_id(1) {
            break r;
        }
        thread::yield_now();
    };

    assert!(resp.is_success());
    assert_eq!(resp.data_len, test_data.len() as u64);

    // Verify uppercase result
    let buffer = client_shm.data_buffer(slot_id).unwrap();
    let result = &buffer[0..test_data.len()];
    assert_eq!(result, b"HELLO WORLD");

    daemon_handle.join().unwrap();
    cleanup_shm(&shm_name);
}

#[test]
fn test_multiple_clients() {
    let shm_name = unique_shm_name("multi_client");

    let config = ShmConfig {
        num_slots: 4,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let daemon_shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    daemon_shm.set_daemon_ready(1);

    let shm_name_clone = shm_name.clone();

    // Daemon thread: process requests from multiple slots
    let daemon_handle = thread::spawn(move || {
        let shm = SharedMemoryRegion::attach(&shm_name_clone).unwrap();
        let mut total_processed = 0;

        // Process until we've handled 12 requests (3 from each of 4 clients)
        while total_processed < 12 {
            for slot_id in 0..4 {
                if let Ok(slot) = shm.slot_control(slot_id) {
                    if slot.client_pid.load(Ordering::Acquire) == 0 {
                        continue;
                    }

                    let req_ring = RequestRing::new(&shm, slot_id).unwrap();
                    let resp_ring = ResponseRing::new(&shm, slot_id).unwrap();

                    if let Some(req) = req_ring.try_pop() {
                        // Return slot_id * 100 + request_id as result
                        let result = (slot_id as u64) * 100 + req.request_id;
                        let resp = ResponseEntry::success(req.request_id, result);
                        while !resp_ring.push(&resp) {
                            thread::yield_now();
                        }
                        total_processed += 1;
                    }
                }
            }
            thread::yield_now();
        }
    });

    // Spawn 4 client threads
    let mut client_handles = vec![];
    for client_id in 0..4 {
        let shm_name_client = shm_name.clone();
        let handle = thread::spawn(move || {
            let shm = SharedMemoryRegion::attach(&shm_name_client).unwrap();
            let slot_id = shm.allocate_slot().unwrap();

            let slot = shm.slot_control(slot_id).unwrap();
            slot.client_pid.store(std::process::id(), Ordering::Release);

            let req_ring = RequestRing::new(&shm, slot_id).unwrap();
            let resp_ring = ResponseRing::new(&shm, slot_id).unwrap();

            // Send 3 requests
            for i in 1..=3 {
                let mut req = RequestEntry::new();
                req.request_id = i;
                while !req_ring.push(&req) {
                    thread::yield_now();
                }

                // Wait for response
                let resp = loop {
                    if let Some(r) = resp_ring.try_pop_by_id(i) {
                        break r;
                    }
                    thread::yield_now();
                };

                assert!(resp.is_success());
                assert_eq!(resp.request_id, i);
                // Verify result encodes our slot_id
                assert_eq!(resp.result, (slot_id as u64) * 100 + i);
            }

            slot_id
        });
        client_handles.push(handle);
    }

    // Wait for all clients
    let mut allocated_slots: Vec<u32> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    allocated_slots.sort();

    // Verify all slots were used
    assert_eq!(allocated_slots, vec![0, 1, 2, 3]);

    daemon_handle.join().unwrap();
    cleanup_shm(&shm_name);
}

#[test]
fn test_shutdown_flag() {
    let shm_name = unique_shm_name("shutdown");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);

    assert!(!shm.is_shutdown_requested());

    shm.request_shutdown();

    assert!(shm.is_shutdown_requested());

    // Attach and verify shutdown is visible
    let client_shm = SharedMemoryRegion::attach(&shm_name).unwrap();
    assert!(client_shm.is_shutdown_requested());

    cleanup_shm(&shm_name);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[test]
fn test_invalid_slot_access() {
    let shm_name = unique_shm_name("invalid_slot");

    let config = ShmConfig {
        num_slots: 2,
        data_buffer_size: 1024,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);

    // Access invalid slot
    assert!(shm.slot_control(99).is_err());
    assert!(shm.data_buffer(99).is_err());

    cleanup_shm(&shm_name);
}

#[test]
fn test_attach_nonexistent_shm() {
    let result = SharedMemoryRegion::attach("nonexistent_shm_12345");
    assert!(result.is_err());
}

#[test]
fn test_long_path_in_data_buffer() {
    let shm_name = unique_shm_name("long_path");

    let config = ShmConfig {
        num_slots: 1,
        data_buffer_size: 4096,
        request_ring_size: 8,
        response_ring_size: 8,
    };

    let shm = SharedMemoryRegion::create(&shm_name, config).unwrap();
    shm.set_daemon_ready(1);
    let slot_id = shm.allocate_slot().unwrap();

    // Create a path longer than INLINE_PATH_LEN
    let long_path = format!("/{}", "a".repeat(INLINE_PATH_LEN + 100));

    // Write to data buffer
    {
        let buffer = shm.data_buffer_mut(slot_id).unwrap();
        buffer[0..long_path.len()].copy_from_slice(long_path.as_bytes());
    }

    // Create request with path in data buffer
    let mut req = RequestEntry::new();
    req.request_id = 1;
    req.op_type = OpType::Open as u32;
    req.data_offset = 0;
    req.path_len = long_path.len() as u32;
    // path field left empty since path is in data buffer

    let req_ring = RequestRing::new(&shm, slot_id).unwrap();
    assert!(req_ring.push(&req));

    // Verify we can pop and reconstruct the path
    let popped = req_ring.try_pop().unwrap();
    assert_eq!(popped.path_len as usize, long_path.len());

    // Read path from data buffer
    let buffer = shm.data_buffer(slot_id).unwrap();
    let path_bytes = &buffer[0..popped.path_len as usize];
    let reconstructed_path = std::str::from_utf8(path_bytes).unwrap();
    assert_eq!(reconstructed_path, long_path);

    cleanup_shm(&shm_name);
}
