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

/// Inode-file v2 extension (32 bytes, little-endian). Persists fields the
/// BenchFS RPC layer carries but `PosixMetadataHeader` does not: POSIX `mode`,
/// inode type discriminator, the on-disk path length, and a schema version.
///
/// Lives at byte offset `EXT2_OFFSET = 32` inside the MSIZE region of every
/// inode file (see `InodeStore`). For chunk-0 of regular data files this
/// extension is **not** used (chunk data lives at offset 0 in the chunk
/// tree). The split-inode layout (case Y of `PLAN_METADATA_PERSISTENCE.md`)
/// stores `BenchfsInodeExtension2` exclusively in the parallel `inodes/`
/// tree, leaving chunk files untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoBytes, FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BenchfsInodeExtension2 {
    /// POSIX mode bits (e.g. `S_IFREG | 0o644`).
    pub mode: u32,
    /// 1 = regular file, 2 = directory, 3 = symlink. Mirrors `InodeType`.
    pub inode_type: u8,
    pub _padding0: u8,
    /// UTF-8 byte length of the path stored at `PATH_OFFSET`. Must satisfy
    /// `path_len as u64 <= MSIZE - PATH_OFFSET`.
    pub path_len: u16,
    /// Schema version, starts at 1.
    pub version: u32,
    pub _reserved: [u8; 20],
}

/// Byte offset of [`BenchfsInodeExtension2`] within the inode-file MSIZE region.
pub const EXT2_OFFSET: u64 = 32;

/// Byte offset where the inline path string begins inside the inode file.
/// Bytes from this offset to `MSIZE - 1` carry the UTF-8 path padded with NULs.
pub const PATH_OFFSET: u64 = 64;

/// Maximum number of UTF-8 bytes a path can occupy in the inline region.
pub const MAX_INLINE_PATH_BYTES: usize = (MSIZE - PATH_OFFSET) as usize;

/// On-disk inode discriminator, mirrors `metadata::types::InodeType` for the
/// disk schema. We avoid pulling the in-memory enum into `storage` to keep
/// the module boundary clean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnDiskInodeType {
    File = 1,
    Directory = 2,
    Symlink = 3,
}

impl OnDiskInodeType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::File),
            2 => Some(Self::Directory),
            3 => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// Decoded view of an on-disk inode file. Produced by [`decode_inode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnDiskInode {
    pub path: String,
    pub inode_type: OnDiskInodeType,
    pub mode: u32,
    pub chunk_size: u64,
    pub flags: u16,
    /// Logical file size (regular files only; 0 for dirs/symlinks).
    pub logical_size: u64,
}

impl BenchfsInodeExtension2 {
    pub fn new(mode: u32, inode_type: OnDiskInodeType, path_len: u16) -> Self {
        Self {
            mode,
            inode_type: inode_type as u8,
            _padding0: 0,
            path_len,
            version: 1,
            _reserved: [0; 20],
        }
    }

    pub fn from_bytes_validated(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < std::mem::size_of::<Self>() {
            return None;
        }
        Self::read_from_bytes(&bytes[..std::mem::size_of::<Self>()]).ok()
    }
}

/// Encode a full 4 KiB inode buffer ready for a single pwrite at offset 0.
///
/// Layout:
/// - `0..16`: [`PosixMetadataHeader`]
/// - `16..32`: [`BenchfsChunk0Extension`] (carries `logical_size`)
/// - `32..64`: [`BenchfsInodeExtension2`] (mode, inode_type, path_len, version)
/// - `64..MSIZE`: UTF-8 path bytes padded with NULs
///
/// Returns `None` if `path.len() > MAX_INLINE_PATH_BYTES`.
pub fn encode_inode(
    path: &str,
    inode_type: OnDiskInodeType,
    mode: u32,
    chunk_size: u64,
    logical_size: u64,
    flags: u16,
) -> Option<Vec<u8>> {
    let path_bytes = path.as_bytes();
    if path_bytes.len() > MAX_INLINE_PATH_BYTES {
        return None;
    }
    let path_len = path_bytes.len() as u16;

    let mut buf = vec![0u8; MSIZE as usize];

    let header = PosixMetadataHeader::new(chunk_size, flags);
    buf[..16].copy_from_slice(header.as_bytes());

    let ext = BenchfsChunk0Extension::with_size(logical_size);
    buf[EXT_OFFSET as usize..EXT_OFFSET as usize + 16].copy_from_slice(ext.as_bytes());

    let ext2 = BenchfsInodeExtension2::new(mode, inode_type, path_len);
    buf[EXT2_OFFSET as usize..EXT2_OFFSET as usize + 32].copy_from_slice(ext2.as_bytes());

    let start = PATH_OFFSET as usize;
    buf[start..start + path_bytes.len()].copy_from_slice(path_bytes);

    Some(buf)
}

/// Decode a 4 KiB inode buffer. Returns `None` on any of:
/// - buffer shorter than MSIZE,
/// - `PosixMetadataHeader.msize` mismatch,
/// - unknown `inode_type`,
/// - `path_len` past the inline budget,
/// - non-UTF-8 path bytes.
pub fn decode_inode(buf: &[u8]) -> Option<OnDiskInode> {
    if buf.len() < MSIZE as usize {
        return None;
    }
    let header = PosixMetadataHeader::from_bytes_validated(&buf[..16])?;
    let ext = BenchfsChunk0Extension::from_bytes_validated(&buf[EXT_OFFSET as usize..])?;
    let ext2 = BenchfsInodeExtension2::from_bytes_validated(&buf[EXT2_OFFSET as usize..])?;

    let inode_type = OnDiskInodeType::from_u8(ext2.inode_type)?;
    let path_len = ext2.path_len as usize;
    if path_len > MAX_INLINE_PATH_BYTES {
        return None;
    }
    let start = PATH_OFFSET as usize;
    let path = std::str::from_utf8(&buf[start..start + path_len])
        .ok()?
        .to_owned();

    Some(OnDiskInode {
        path,
        inode_type,
        mode: ext2.mode,
        chunk_size: header.chunk_size,
        flags: header.flags,
        logical_size: ext.logical_size,
    })
}

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

    #[test]
    fn ext2_struct_size_is_32() {
        assert_eq!(std::mem::size_of::<BenchfsInodeExtension2>(), 32);
        assert_eq!(EXT2_OFFSET, 32);
        assert_eq!(PATH_OFFSET, 64);
    }

    #[test]
    fn inode_round_trip_file() {
        let buf = encode_inode(
            "/foo/bar/baz.txt",
            OnDiskInodeType::File,
            0o100644,
            4 * 1024 * 1024,
            12345,
            0,
        )
        .unwrap();
        assert_eq!(buf.len(), MSIZE as usize);
        let decoded = decode_inode(&buf).unwrap();
        assert_eq!(decoded.path, "/foo/bar/baz.txt");
        assert_eq!(decoded.inode_type, OnDiskInodeType::File);
        assert_eq!(decoded.mode, 0o100644);
        assert_eq!(decoded.chunk_size, 4 * 1024 * 1024);
        assert_eq!(decoded.logical_size, 12345);
        assert_eq!(decoded.flags, 0);
    }

    #[test]
    fn inode_round_trip_dir() {
        let buf = encode_inode(
            "/path/to/dir",
            OnDiskInodeType::Directory,
            0o040755,
            0,
            0,
            0,
        )
        .unwrap();
        let decoded = decode_inode(&buf).unwrap();
        assert_eq!(decoded.inode_type, OnDiskInodeType::Directory);
        assert_eq!(decoded.mode, 0o040755);
    }

    #[test]
    fn inode_rejects_overlong_path() {
        let long = "a".repeat(MAX_INLINE_PATH_BYTES + 1);
        assert!(encode_inode(&long, OnDiskInodeType::File, 0o644, 0, 0, 0).is_none());
    }

    #[test]
    fn inode_decode_rejects_short_buffer() {
        let buf = vec![0u8; 100];
        assert!(decode_inode(&buf).is_none());
    }

    #[test]
    fn inode_decode_rejects_bad_msize() {
        let mut buf = encode_inode("/x", OnDiskInodeType::File, 0o644, 4096, 0, 0).unwrap();
        // Corrupt msize field (offset 8..10).
        buf[8] = 0;
        buf[9] = 0;
        assert!(decode_inode(&buf).is_none());
    }
}
