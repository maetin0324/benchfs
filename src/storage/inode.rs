// CHFS POSIX-style on-disk metadata header.
//
// Each chunk file is laid out as `[ MSIZE-byte header ][ chunk data ... ]`.
// The header's first 16 bytes match CHFS's `struct metadata` byte-for-byte
// (see chfs/chfsd/fs_posix.c). The remaining bytes up to MSIZE are reserved
// padding so that data starts on a 4 KiB boundary, which lets O_DIRECT and
// io_uring's `read_fixed` / `write_fixed` continue to operate on chunk
// offsets without per-request alignment juggling.
//
// Reference (chfs/chfsd/fs_posix.c):
// ```c
// struct metadata {
//     size_t chunk_size;   // 8 bytes (u64 on 64-bit Linux)
//     uint16_t msize;      // 2 bytes
//     uint16_t flags;      // 2 bytes
//     // 4 bytes trailing padding (C natural alignment for size_t)
// };
// static int msize = sizeof(struct metadata); // == 16 in CHFS
// ```
//
// BenchFS records `MSIZE` (4096) as the on-disk `msize`; CHFS reads `msize`
// from the header itself and validates it against its own static, so the
// two layouts are not wire-compatible — but BenchFS only ever reads files
// it wrote, and the architectural pattern (per-chunk file, prefix header,
// metadata co-located with chunk-0 storage) matches CHFS.

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// On-disk reserved region at file offset 0 of every chunk. Padded up to a
/// 4 KiB boundary so chunk data starts page-aligned (preserves O_DIRECT and
/// io_uring fixed-buffer alignment for aligned chunk-relative offsets).
pub const MSIZE: u64 = 4096;

/// CHFS POSIX-style chunk metadata header (16 bytes, little-endian).
///
/// Layout matches CHFS's `struct metadata` exactly:
///   - `chunk_size` (u64): the chunk size advertised at file create time.
///   - `msize` (u16): always [`MSIZE`]; CHFS verifies this on read.
///   - `flags` (u16): CHFS cache flags (CHFS_FS_CACHE / CHFS_FS_DIRTY).
///   - 4 bytes of trailing padding to keep `size_of::<Self>() == MSIZE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct PosixMetadataHeader {
    pub chunk_size: u64,
    pub msize: u16,
    pub flags: u16,
    pub _padding: u32,
}

/// BenchFS-specific extension stored immediately after [`PosixMetadataHeader`]
/// inside the MSIZE-byte reserved region. Only meaningful on chunk 0; for
/// chunks 1+ these bytes are written as zero and ignored.
///
/// CHFS POSIX has no analogue: in CHFS the logical file size is computed by
/// the client iterating chunks. BenchFS persists `logical_size` here so the
/// metadata RPC handler can answer `MetadataLookup` with a single chunk-0
/// stat, matching the latency characteristics of the previous in-memory
/// implementation while keeping all metadata on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BenchfsChunk0Extension {
    /// Logical file size in bytes (max-extend semantics on `MetadataUpdate`).
    pub logical_size: u64,
    /// Reserved for future use.
    pub _reserved: [u8; 8],
}

/// Byte offset of [`BenchfsChunk0Extension`] within the chunk-0 MSIZE region.
pub const EXT_OFFSET: u64 = 16;

impl BenchfsChunk0Extension {
    pub fn zero() -> Self {
        Self {
            logical_size: 0,
            _reserved: [0; 8],
        }
    }

    pub fn with_size(size: u64) -> Self {
        Self {
            logical_size: size,
            _reserved: [0; 8],
        }
    }

    pub fn from_bytes_validated(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < std::mem::size_of::<Self>() {
            return None;
        }
        Self::read_from_bytes(&bytes[..std::mem::size_of::<Self>()]).ok()
    }
}

impl PosixMetadataHeader {
    /// Construct a fresh header for a new chunk file.
    pub fn new(chunk_size: u64, flags: u16) -> Self {
        Self {
            chunk_size,
            msize: MSIZE as u16,
            flags,
            _padding: 0,
        }
    }

    /// Encode the header into an MSIZE-byte buffer suitable for a single
    /// O_DIRECT-aligned `pwrite` at offset 0. The struct's 16 bytes occupy
    /// the prefix; the remaining bytes are zero-padded.
    pub fn as_msize_buffer(&self) -> Vec<u8> {
        let mut out = vec![0u8; MSIZE as usize];
        out[..std::mem::size_of::<Self>()].copy_from_slice(self.as_bytes());
        out
    }

    /// Decode a header read from offset 0. Accepts buffers of any size
    /// >= 16 (the actual struct size); only the first 16 bytes are parsed.
    /// Returns `None` if the buffer is too short or if the recorded `msize`
    /// disagrees with [`MSIZE`] (matching CHFS's `get_metadata` check).
    pub fn from_bytes_validated(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < std::mem::size_of::<Self>() {
            return None;
        }
        let header = Self::read_from_bytes(&bytes[..std::mem::size_of::<Self>()]).ok()?;
        if header.msize as u64 != MSIZE {
            return None;
        }
        Some(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_struct_matches_chfs() {
        // The struct itself is 16 bytes (CHFS struct metadata layout).
        assert_eq!(std::mem::size_of::<PosixMetadataHeader>(), 16);
        // The on-disk reserved region is 4 KiB (page-aligned).
        assert_eq!(MSIZE, 4096);
    }

    #[test]
    fn round_trip() {
        let h = PosixMetadataHeader::new(4 * 1024 * 1024, 0);
        let bytes = h.as_msize_buffer();
        assert_eq!(bytes.len(), MSIZE as usize);
        let parsed = PosixMetadataHeader::from_bytes_validated(&bytes).unwrap();
        assert_eq!(parsed.chunk_size, 4 * 1024 * 1024);
        assert_eq!(parsed.msize as u64, MSIZE);
        assert_eq!(parsed.flags, 0);
    }

    #[test]
    fn rejects_wrong_msize() {
        let mut h = PosixMetadataHeader::new(64 * 1024, 0);
        h.msize = 8;
        let bytes = h.as_msize_buffer();
        assert!(PosixMetadataHeader::from_bytes_validated(&bytes).is_none());
    }

    #[test]
    fn rejects_short_buffer() {
        let bytes = [0u8; 8];
        assert!(PosixMetadataHeader::from_bytes_validated(&bytes).is_none());
    }
}
