// BenchFS Mini Server Daemon
//
// Standalone server process that can be started separately from IOR clients.
// Servers register themselves in a shared registry and handle client requests.

use std::ffi::CString;
use std::process;

const BENCHFS_MINI_SUCCESS: i32 = 0;

fn main() {
    eprintln!("[benchfsd_mini] Starting BenchFS Mini Server");

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();

    let mut registry_dir = "/shared/registry_mini";
    let mut is_server = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--registry" => {
                if i + 1 < args.len() {
                    registry_dir = &args[i + 1];
                    i += 1;
                } else {
                    eprintln!("[benchfsd_mini] ERROR: --registry requires an argument");
                    process::exit(1);
                }
            }
            "--server" => {
                is_server = true;
            }
            "--help" | "-h" => {
                println!("BenchFS Mini Server Daemon");
                println!();
                println!("USAGE:");
                println!("    benchfsd_mini [OPTIONS]");
                println!();
                println!("OPTIONS:");
                println!("    --registry <PATH>    Registry directory path (default: /shared/registry_mini)");
                println!("    --server             Run as server (required)");
                println!("    --help, -h           Print this help message");
                process::exit(0);
            }
            _ => {
                eprintln!("[benchfsd_mini] ERROR: Unknown argument: {}", args[i]);
                eprintln!("[benchfsd_mini] Use --help for usage information");
                process::exit(1);
            }
        }
        i += 1;
    }

    if !is_server {
        eprintln!("[benchfsd_mini] ERROR: --server flag is required");
        eprintln!("[benchfsd_mini] Use --help for usage information");
        process::exit(1);
    }

    eprintln!("[benchfsd_mini] Configuration:");
    eprintln!("[benchfsd_mini]   Registry: {}", registry_dir);
    eprintln!("[benchfsd_mini]   Mode: Server");

    // Initialize BenchFS Mini as server
    let c_registry_dir = CString::new(registry_dir).unwrap();

    eprintln!("[benchfsd_mini] Initializing...");
    let ret = unsafe { benchfs::benchfs_mini_lib::benchfs_mini_init(c_registry_dir.as_ptr(), true) };

    if ret != BENCHFS_MINI_SUCCESS {
        eprintln!("[benchfsd_mini] ERROR: Failed to initialize (error code: {})", ret);
        process::exit(1);
    }

    eprintln!("[benchfsd_mini] Initialized successfully");

    // Start server loop
    eprintln!("[benchfsd_mini] Starting server loop...");
    let ret = unsafe { benchfs::benchfs_mini_lib::benchfs_mini_start_server() };

    if ret != BENCHFS_MINI_SUCCESS {
        eprintln!("[benchfsd_mini] ERROR: Server loop failed (error code: {})", ret);
        unsafe { benchfs::benchfs_mini_lib::benchfs_mini_finalize() };
        process::exit(1);
    }

    eprintln!("[benchfsd_mini] Server started successfully");

    // Keep the process alive
    // In a real implementation, this would wait for a signal to terminate
    // For now, we'll just sleep indefinitely
    eprintln!("[benchfsd_mini] Server is running. Press Ctrl+C to stop.");
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));

        // Progress UCX
        unsafe {
            benchfs::benchfs_mini_lib::benchfs_mini_progress();
        }
    }
}
