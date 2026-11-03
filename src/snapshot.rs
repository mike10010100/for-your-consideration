#![forbid(unsafe_code)]

//! # Disk Snapshot Persistence & Fast Boot Engine
//!
//! Provides atomic, CRC32-verified binary serialization and deserialization
//! of [`StringInterner`] and [`GraphStore`].
//!
//! ## Format Specification
//!
//! The snapshot file uses a compact self-describing binary format:
//! - **64-byte Header**: Magic bytes (`b"FYFD"`), format version (`1`), created timestamp,
//!   Jetstream cursor, entry counts, payload CRC32, and header CRC32.
//! - **Section 1: String Dictionary**: Length-prefixed UTF-8 strings.
//! - **Section 2: Forward User Interactions**: `(user_id, [CompactEdge])`.
//! - **Section 3: Reverse Post Interactions**: `(post_id, [CompactEdge])`.
//! - **Section 4: User Likes `RoaringBitmaps`**: `(user_id, serialized_bitmap)`.
//! - **Section 5: Follow Relationships**: `(follower_id, [followed_id])`.
//! - **Section 6: Post Metadata**: `(post_id, author_id, root_id, parent_id, created_at, is_nsfw)`.
//! - **Section 7: Active Recent Posts**: `(post_id, last_activity_timestamp)`.
//! - **Section 8 (v2+): User Preferences**: fixed-width per-user dial records.
//!
//! ## Streaming Load Pipeline
//!
//! Loading is fully streaming (two bounded passes over the file):
//! 1. **Integrity pass**: the payload region is streamed through a CRC32 hasher using a
//!    fixed 1 MiB chunk buffer — the payload is never materialized in RAM, so boot-time
//!    memory is independent of snapshot size (the save path streams shard-by-shard the
//!    same way).
//! 2. **Parse pass**: sections are deserialized directly from the file into in-memory
//!    stores via [`StreamReader`]. Truncated payloads surface as `FeedError::Snapshot`
//!    with an explicit EOF marker.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use crc32fast::Hasher;
use roaring::RoaringBitmap;
use tracing::info;

use crate::error::{FeedError, Result};
use crate::graph::{GraphSnapshotData, GraphStore};
use crate::interner::{string_shard_idx, StringInterner, NUM_INTERNER_SHARDS};
use crate::preferences::UserPreferencesStore;
use crate::recommender::ImpressionStore;
use crate::types::{
    CompactEdge, PostMeta, SnapshotStatusInfo, TopicWeights, UserDials, DEFAULT_MIN_LIKES,
};

/// Magic 4-byte header identifier: `b"FYFD"` (For-You Feed).
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"FYFD";

/// Current snapshot format version (5 adds per-post `is_nsfw` and per-user `no_nsfw`).
pub const SNAPSHOT_FORMAT_VERSION: u16 = 5;

/// Legacy snapshot format version 4 with `include_replies` and `min_likes` but no NSFW flags.
pub const SNAPSHOT_FORMAT_VERSION_V4: u16 = 4;

/// Legacy snapshot format version 3 with `include_replies` without `min_likes`.
pub const SNAPSHOT_FORMAT_VERSION_V3: u16 = 3;

/// Legacy snapshot format version 2 with user preferences.
pub const SNAPSHOT_FORMAT_VERSION_V2: u16 = 2;

/// Legacy snapshot format version 1 without user preferences.
pub const SNAPSHOT_FORMAT_VERSION_V1: u16 = 1;

/// Fixed header size in bytes.
pub const HEADER_SIZE: usize = 64;

/// Snapshot configuration.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Path to the primary snapshot binary file.
    pub path: PathBuf,
    /// Periodic snapshot checkpoint interval in seconds (default: 300s = 5m).
    pub interval_secs: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("snapshot.bin"),
            interval_secs: 300,
        }
    }
}

/// Metadata header describing snapshot contents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Magic 4-byte signature (`b"FYFD"`).
    pub magic: [u8; 4],
    /// Snapshot format version.
    pub format_version: u16,
    /// Length of fixed header in bytes (64).
    pub header_length: u16,
    /// Unix timestamp in seconds when snapshot was created.
    pub created_at_secs: u64,
    /// Jetstream firehose cursor timestamp in microseconds.
    pub jetstream_cursor_us: u64,
    /// Reserved flags (0).
    pub flags: u32,
    /// Number of interned strings.
    pub num_strings: u32,
    /// Number of distinct interacting users.
    pub num_users: u32,
    /// Total forward interaction edges.
    pub total_forward_edges: u64,
    /// Number of distinct followers.
    pub num_followers: u32,
    /// Number of post metadata entries.
    pub num_post_metadata: u32,
    /// CRC32 checksum over the uncompressed payload.
    pub payload_crc32: u32,
    /// CRC32 checksum over header bytes `0..56`.
    pub header_crc32: u32,
    /// Number of saved user preference profiles.
    pub num_preferences: u32,
}

/// Result of loading a snapshot.
#[derive(Debug)]
pub struct LoadedSnapshot {
    /// Deserialized snapshot header metadata.
    pub header: SnapshotHeader,
    /// Time taken to load, verify, and hydrate memory structures in milliseconds.
    pub load_duration_ms: f64,
}

/// Thread-safe tracker for snapshot persistence lifecycle, durations, and file metrics.
#[derive(Debug)]
pub struct SnapshotStatusTracker {
    inner: parking_lot::RwLock<SnapshotStatusInfo>,
    path: PathBuf,
}

impl Default for SnapshotStatusTracker {
    fn default() -> Self {
        Self::new(&SnapshotConfig::default())
    }
}

impl SnapshotStatusTracker {
    /// Creates a new [`SnapshotStatusTracker`] with configuration defaults.
    #[must_use]
    pub fn new(config: &SnapshotConfig) -> Self {
        let file_size_bytes = config.path.metadata().map_or(0, |m| m.len());
        let status = if config.path.exists() {
            "persisted".to_string()
        } else {
            "clean".to_string()
        };
        let info = SnapshotStatusInfo {
            status,
            last_saved_secs: 0,
            last_saved_ago_secs: 0,
            last_load_duration_ms: 0.0,
            last_save_duration_ms: 0.0,
            interval_secs: config.interval_secs,
            file_path: config.path.display().to_string(),
            file_size_bytes,
            format_version: SNAPSHOT_FORMAT_VERSION,
        };
        Self {
            inner: parking_lot::RwLock::new(info),
            path: config.path.clone(),
        }
    }

    /// Records successful boot/startup snapshot hydration.
    pub fn record_load(&self, duration_ms: f64) {
        let mut guard = self.inner.write();
        guard.status = "hydrated".to_string();
        guard.last_load_duration_ms = duration_ms;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        guard.last_saved_secs = now;
        if let Ok(meta) = self.path.metadata() {
            guard.file_size_bytes = meta.len();
        }
    }

    /// Records successful periodic or shutdown snapshot save.
    pub fn record_save(&self, duration_ms: f64, file_size_bytes: u64) {
        let mut guard = self.inner.write();
        guard.status = "persisted".to_string();
        guard.last_save_duration_ms = duration_ms;
        guard.file_size_bytes = file_size_bytes;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        guard.last_saved_secs = now;
    }

    /// Records snapshot persistence failure.
    pub fn record_save_failure(&self, err: &str) {
        let mut guard = self.inner.write();
        guard.status = format!("error: {err}");
    }

    /// Manually sets the status string.
    pub fn set_status(&self, status: impl Into<String>) {
        let mut guard = self.inner.write();
        guard.status = status.into();
    }

    /// Returns a point-in-time [`SnapshotStatusInfo`] snapshot with live calculated elapsed seconds.
    #[must_use]
    pub fn get_status(&self) -> SnapshotStatusInfo {
        let mut info = self.inner.read().clone();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        if info.last_saved_secs > 0 {
            info.last_saved_ago_secs = now.saturating_sub(info.last_saved_secs);
        }
        if let Ok(meta) = self.path.metadata() {
            info.file_size_bytes = meta.len();
        }
        info
    }
}

/// Size of the bounded chunk buffer used to stream the payload during CRC verification
/// and deserialization (1 MiB). Peak load memory is bounded by this buffer instead of
/// scaling with the snapshot file size.
const STREAM_CHUNK_SIZE: usize = 1024 * 1024;

/// Streaming reader over a [`BufReader`] mirroring the [`ByteSliceReader`] section API
/// without materializing the payload in memory.
///
/// Tracks the number of bytes remaining in the payload region so hostile length
/// prefixes are rejected *before* any allocation, never aborting the process on a
/// malicious snapshot (the old zero-copy reader got this for free from slice bounds;
/// a streaming reader must enforce the same bound explicitly).
struct StreamReader<R: Read> {
    inner: R,
    /// One-byte pushback buffer used by [`StreamReader::has_more`].
    pending_byte: Option<u8>,
    /// Bytes remaining between the current stream position and the end of the
    /// snapshot payload region (everything after the 64-byte header).
    remaining_payload: u64,
}

impl<R: Read> StreamReader<R> {
    const fn new(inner: R, payload_len: u64) -> Self {
        Self {
            inner,
            pending_byte: None,
            remaining_payload: payload_len,
        }
    }

    /// Validates that a requested length prefix is backed by enough remaining payload
    /// bytes, rejecting malicious oversized prefixes before any allocation.
    fn check_len(&self, len: u64, what: &str) -> Result<()> {
        if len > self.remaining_payload {
            return Err(FeedError::Snapshot(format!(
                "Unexpected EOF: {what} length prefix {len} exceeds remaining payload ({})",
                self.remaining_payload
            )));
        }
        Ok(())
    }

    /// Reads exactly `buf.len()` bytes into `buf`, draining the one-byte pushback
    /// buffer first so `has_more` probing never misaligns subsequent reads.
    ///
    /// Truncated payloads surface as [`FeedError::Snapshot`] (rather than raw I/O errors)
    /// with an explicit "Unexpected EOF" marker so callers can distinguish corruption
    /// from environmental I/O failure.
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let filled = self.pending_byte.take().map_or(0, |b| {
            buf[0] = b;
            1
        });
        if filled < buf.len() {
            self.inner
                .read_exact(&mut buf[filled..])
                .map_err(|e| FeedError::Snapshot(format!("Unexpected EOF: {e}")))?;
        }
        self.remaining_payload = self.remaining_payload.saturating_sub(buf.len() as u64);
        Ok(())
    }

    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    fn read_f32(&mut self) -> Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_le_bytes(buf))
    }

    fn read_exact_vec(&mut self, len: usize, what: &str) -> Result<Vec<u8>> {
        self.check_len(len as u64, what)?;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_edges(&mut self, count: usize) -> Result<Vec<CompactEdge>> {
        let byte_len = count
            .checked_mul(8)
            .ok_or_else(|| FeedError::Snapshot("Edge count overflow".to_string()))?;
        let bytes = self.read_exact_vec(byte_len, "edge array")?;
        let mut edges = Vec::with_capacity(count);
        for chunk in bytes.as_chunks::<8>().0 {
            let target = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let packed = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
            edges.push(CompactEdge { target, packed });
        }
        Ok(edges)
    }

    fn read_u32_vec(&mut self, count: usize) -> Result<Vec<u32>> {
        let byte_len = count
            .checked_mul(4)
            .ok_or_else(|| FeedError::Snapshot("u32 count overflow".to_string()))?;
        let bytes = self.read_exact_vec(byte_len, "u32 array")?;
        let mut list = Vec::with_capacity(count);
        for chunk in bytes.as_chunks::<4>().0 {
            let val = u32::from_le_bytes(*chunk);
            list.push(val);
        }
        Ok(list)
    }

    fn read_string_buf(&mut self, len: usize, buf: &mut Vec<u8>) -> Result<()> {
        self.check_len(len as u64, "string")?;
        buf.resize(len, 0);
        self.read_exact(buf)?;
        Ok(())
    }

    fn read_bitmap(&mut self, len: usize) -> Result<Vec<u8>> {
        self.read_exact_vec(len, "roaring bitmap")
    }

    /// Bounds a hostile record count to the physical payload capacity: no section can
    /// contain more records than there are bytes left, each record being at least
    /// `min_record_bytes` long. Prevents attacker-inflated counts from driving huge
    /// `Vec::with_capacity` reservations before any validation.
    fn bound_count(&self, count: usize, min_record_bytes: u64) -> usize {
        let max_by_payload = (self.remaining_payload / min_record_bytes.max(1)) as usize;
        count.min(max_by_payload)
    }

    /// Returns `true` if at least one more byte is available in the underlying stream.
    fn has_more(&mut self) -> Result<bool> {
        if self.pending_byte.is_some() {
            return Ok(true);
        }
        if self.remaining_payload == 0 {
            return Ok(false);
        }
        let mut probe = [0u8; 1];
        let read = self
            .inner
            .read(&mut probe)
            .map_err(|e| FeedError::Snapshot(format!("Unexpected EOF: {e}")))?;
        debug_assert!(read <= 1);
        if read == 1 {
            self.pending_byte = Some(probe[0]);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Streams the payload region (`HEADER_SIZE..file_len`) through a CRC32 hasher using a
/// bounded 1 MiB chunk buffer and returns the computed checksum.
///
/// Memory usage is constant regardless of snapshot size; the payload is never fully
/// materialized in RAM.
fn compute_payload_crc_streaming(reader: &mut BufReader<File>, file_len: u64) -> Result<u32> {
    reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
    let payload_len = file_len.saturating_sub(HEADER_SIZE as u64);
    let mut hasher = Hasher::new();
    let mut chunk = vec![0u8; STREAM_CHUNK_SIZE];
    let mut remaining = payload_len;

    while remaining > 0 {
        let take = remaining.min(STREAM_CHUNK_SIZE as u64) as usize;
        reader.read_exact(&mut chunk[..take]).map_err(|e| {
            FeedError::Snapshot(format!("Unexpected EOF verifying payload CRC: {e}"))
        })?;
        hasher.update(&chunk[..take]);
        remaining -= take as u64;
    }

    Ok(hasher.finalize())
}

/// Atomically saves the interner, graph, and Jetstream cursor to disk with empty user preferences.
///
/// Convenience wrapper around [`save_snapshot_with_preferences`].
pub fn save_snapshot(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    jetstream_cursor_us: u64,
) -> Result<SnapshotHeader> {
    let empty_preferences = UserPreferencesStore::new();
    save_snapshot_with_preferences(
        path,
        interner,
        graph,
        &empty_preferences,
        jetstream_cursor_us,
    )
}

/// Atomically saves the interner, graph, user preferences, and Jetstream cursor to disk.
///
/// 1. Writes binary data across Sections 1–8 and computes payload CRC32 into a temporary file (`snapshot.bin.tmp`).
/// 2. Seeks back to offset 0 and writes the completed 64-byte self-describing header with header CRC32.
/// 3. Flushes and syncs to disk (`sync_all`).
/// 4. Atomically renames the temporary file to destination path.
pub fn save_snapshot_with_preferences(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
    jetstream_cursor_us: u64,
) -> Result<SnapshotHeader> {
    save_snapshot_internal(
        path,
        interner,
        graph,
        preferences,
        None,
        jetstream_cursor_us,
    )
}

/// Atomically saves the interner, graph, user preferences, impression store, and Jetstream cursor to disk.
pub fn save_snapshot_with_impressions(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
    impressions: &ImpressionStore,
    jetstream_cursor_us: u64,
) -> Result<SnapshotHeader> {
    save_snapshot_internal(
        path,
        interner,
        graph,
        preferences,
        Some(impressions),
        jetstream_cursor_us,
    )
}

fn save_snapshot_internal(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
    impressions: Option<&ImpressionStore>,
    jetstream_cursor_us: u64,
) -> Result<SnapshotHeader> {
    let dest_path = path.as_ref();
    let tmp_path = dest_path.file_name().map_or_else(
        || dest_path.with_extension("bin.tmp"),
        |name| {
            let mut tmp_name = name.to_os_string();
            tmp_name.push(".tmp");
            dest_path.with_file_name(tmp_name)
        },
    );

    if let Some(parent) = dest_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let file = File::create(&tmp_path)?;
    let mut writer = BufWriter::with_capacity(128 * 1024, file);

    // 1. Write placeholder 64-byte header
    let zero_header = [0u8; HEADER_SIZE];
    writer.write_all(&zero_header)?;

    // 2. Stream payload and compute CRC32
    let mut hasher = Hasher::new();

    let mut write_chunk = |data: &[u8]| -> std::io::Result<()> {
        writer.write_all(data)?;
        hasher.update(data);
        Ok(())
    };

    // Section 1: Strings
    let num_strings = interner.stream_strings_to(&mut write_chunk)?;

    // Section 2: User Interactions (Forward)
    let (num_users, total_forward_edges) = graph.stream_user_interactions_to(&mut write_chunk)?;

    // Section 3: Post Interactions (Reverse)
    let _num_posts = graph.stream_post_interactions_to(&mut write_chunk)?;

    // Section 4: Roaring Bitmaps
    let mut bm_buf = Vec::with_capacity(64 * 1024);
    let _num_bm_users = graph.stream_user_likes_bitmaps_to(&mut write_chunk, &mut bm_buf)?;

    // Section 5: Follows
    let num_followers = graph.stream_follows_to(&mut write_chunk)?;

    // Section 6: Post Metadata
    let num_post_metadata = graph.stream_post_metadata_to(&mut write_chunk)?;

    // Section 7: Active Recent Posts
    let _num_recent = graph.stream_active_recent_posts_to(&mut write_chunk)?;

    // Section 8: User Preferences (Version 4)
    let num_preferences = preferences.stream_preferences_to(&mut write_chunk)?;

    // Section 9 (optional): Impression History
    if let Some(imp_store) = impressions {
        let _num_impression_viewers = imp_store.stream_impressions_to(&mut write_chunk)?;
    }

    // Finalize CRC32
    let payload_crc32 = hasher.finalize();
    writer.flush()?;

    let created_at_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    // Build 64-byte Header
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header_bytes[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    header_bytes[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header_bytes[8..16].copy_from_slice(&created_at_secs.to_le_bytes());
    header_bytes[16..24].copy_from_slice(&jetstream_cursor_us.to_le_bytes());
    header_bytes[24..28].copy_from_slice(&0u32.to_le_bytes()); // flags
    header_bytes[28..32].copy_from_slice(&num_strings.to_le_bytes());
    header_bytes[32..36].copy_from_slice(&num_users.to_le_bytes());
    header_bytes[36..44].copy_from_slice(&total_forward_edges.to_le_bytes());
    header_bytes[44..48].copy_from_slice(&num_followers.to_le_bytes());
    header_bytes[48..52].copy_from_slice(&num_post_metadata.to_le_bytes());
    header_bytes[52..56].copy_from_slice(&payload_crc32.to_le_bytes());

    // Header CRC over bytes 0..56
    let mut h_hasher = Hasher::new();
    h_hasher.update(&header_bytes[0..56]);
    let header_crc32 = h_hasher.finalize();
    header_bytes[56..60].copy_from_slice(&header_crc32.to_le_bytes());
    header_bytes[60..64].copy_from_slice(&num_preferences.to_le_bytes());

    // 3. Seek to offset 0 and write header
    let mut file = writer
        .into_inner()
        .map_err(std::io::IntoInnerError::into_error)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header_bytes)?;
    file.flush()?;
    file.sync_all()?;
    drop(file);

    // 4. Atomic Rename
    std::fs::rename(&tmp_path, dest_path)?;

    Ok(SnapshotHeader {
        magic: SNAPSHOT_MAGIC,
        format_version: SNAPSHOT_FORMAT_VERSION,
        header_length: HEADER_SIZE as u16,
        created_at_secs,
        jetstream_cursor_us,
        flags: 0,
        num_strings,
        num_users,
        total_forward_edges,
        num_followers,
        num_post_metadata,
        payload_crc32,
        header_crc32,
        num_preferences,
    })
}

/// Loads and hydrates snapshot into the interner and graph with CRC32 integrity verification.
///
/// Returns `Ok(None)` if the snapshot file does not exist.
/// Returns `Err(FeedError::Snapshot(...))` if header magic, version, or CRC32 verification fails.
pub fn load_snapshot(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
) -> Result<Option<LoadedSnapshot>> {
    let preferences = UserPreferencesStore::new();
    load_snapshot_with_preferences(path, interner, graph, &preferences)
}

/// Loads and hydrates snapshot into the interner, graph, and user preferences with CRC32 integrity verification.
///
/// Supports both Version 1 snapshots (without preferences) and Version 2 snapshots (with preferences).
pub fn load_snapshot_with_preferences(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
) -> Result<Option<LoadedSnapshot>> {
    let impressions = ImpressionStore::default();
    load_snapshot_with_impressions(path, interner, graph, preferences, &impressions)
}

/// Loads and hydrates snapshot into the interner, graph, user preferences, and impression store
/// with CRC32 integrity verification.
pub fn load_snapshot_with_impressions(
    path: impl AsRef<Path>,
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
    impressions: &ImpressionStore,
) -> Result<Option<LoadedSnapshot>> {
    let file_path = path.as_ref();
    if !file_path.exists() {
        return Ok(None);
    }

    let start_time = Instant::now();
    let file = File::open(file_path)?;
    let metadata = file.metadata()?;
    if metadata.len() < HEADER_SIZE as u64 {
        return Err(FeedError::Snapshot(format!(
            "Snapshot file '{}' is too small ({} bytes)",
            file_path.display(),
            metadata.len()
        )));
    }

    let mut reader = BufReader::with_capacity(128 * 1024, file);

    // 1. Read and verify Header
    let mut header_buf = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header_buf)?;

    let magic: [u8; 4] = [header_buf[0], header_buf[1], header_buf[2], header_buf[3]];
    if magic != SNAPSHOT_MAGIC {
        return Err(FeedError::Snapshot(format!(
            "Invalid snapshot magic bytes: {magic:?}, expected {SNAPSHOT_MAGIC:?}"
        )));
    }

    let version = u16::from_le_bytes([header_buf[4], header_buf[5]]);
    if version != SNAPSHOT_FORMAT_VERSION_V1
        && version != SNAPSHOT_FORMAT_VERSION_V2
        && version != SNAPSHOT_FORMAT_VERSION_V3
        && version != SNAPSHOT_FORMAT_VERSION_V4
        && version != SNAPSHOT_FORMAT_VERSION
    {
        return Err(FeedError::Snapshot(format!(
            "Unsupported snapshot version {version}, expected {SNAPSHOT_FORMAT_VERSION}"
        )));
    }

    let created_at_secs = u64::from_le_bytes(
        header_buf[8..16]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header created_at_secs".to_string()))?,
    );
    let jetstream_cursor_us = u64::from_le_bytes(
        header_buf[16..24]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header jetstream_cursor_us".to_string()))?,
    );
    let flags = u32::from_le_bytes(
        header_buf[24..28]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header flags".to_string()))?,
    );
    let num_strings = u32::from_le_bytes(
        header_buf[28..32]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header num_strings".to_string()))?,
    );
    let num_users = u32::from_le_bytes(
        header_buf[32..36]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header num_users".to_string()))?,
    );
    let total_forward_edges = u64::from_le_bytes(
        header_buf[36..44]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header total_forward_edges".to_string()))?,
    );
    let num_followers = u32::from_le_bytes(
        header_buf[44..48]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header num_followers".to_string()))?,
    );
    let num_post_metadata = u32::from_le_bytes(
        header_buf[48..52]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header num_post_metadata".to_string()))?,
    );
    let expected_payload_crc = u32::from_le_bytes(
        header_buf[52..56]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header payload_crc".to_string()))?,
    );
    let expected_header_crc = u32::from_le_bytes(
        header_buf[56..60]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header header_crc".to_string()))?,
    );
    let header_num_preferences = u32::from_le_bytes(
        header_buf[60..64]
            .try_into()
            .map_err(|_| FeedError::Snapshot("Corrupt header num_preferences".to_string()))?,
    );

    // Verify Header CRC
    let mut h_hasher = Hasher::new();
    h_hasher.update(&header_buf[0..56]);
    let computed_header_crc = h_hasher.finalize();
    if computed_header_crc != expected_header_crc {
        return Err(FeedError::Snapshot(format!(
            "Header CRC32 checksum mismatch: expected {expected_header_crc:#010x}, calculated {computed_header_crc:#010x}"
        )));
    }

    // 2. Verify payload CRC32 by streaming the payload through a bounded chunk buffer.
    //    The payload is never fully materialized in RAM: peak load memory is bounded by
    //    STREAM_CHUNK_SIZE (1 MiB) plus the deserialized in-memory structures.
    let file_len = metadata.len();
    let actual_payload_crc = compute_payload_crc_streaming(&mut reader, file_len)?;
    if actual_payload_crc != expected_payload_crc {
        return Err(FeedError::Snapshot(format!(
            "Payload CRC32 mismatch: expected {expected_payload_crc:#010x}, calculated {actual_payload_crc:#010x}"
        )));
    }

    // 3. Deserialize Payload section-by-section directly from the file (second streaming pass).
    reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
    let payload_len = file_len.saturating_sub(HEADER_SIZE as u64);
    let mut stream = StreamReader::new(&mut reader, payload_len);

    // Section 1: Strings
    let string_count = stream.read_u32()? as usize;
    let bounded_strings = stream.bound_count(string_count, 4);
    let per_shard_cap = (bounded_strings / NUM_INTERNER_SHARDS).saturating_add(4);
    let mut shard_buckets: [Vec<CompactString>; NUM_INTERNER_SHARDS] =
        std::array::from_fn(|_| Vec::with_capacity(per_shard_cap));
    let mut str_buf = Vec::with_capacity(128);

    for _ in 0..string_count {
        let len = stream.read_u32()? as usize;
        stream.read_string_buf(len, &mut str_buf)?;
        let s = std::str::from_utf8(&str_buf).map_err(|e| {
            FeedError::Snapshot(format!("Invalid UTF-8 in string interner snapshot: {e}"))
        })?;
        let shard_idx = string_shard_idx(s);
        shard_buckets[shard_idx].push(CompactString::new(s));
    }
    interner.hydrate_sharded_buckets(shard_buckets);

    // Section 2: User Interactions
    let user_count = stream.read_u32()? as usize;
    let mut user_interactions = Vec::with_capacity(stream.bound_count(user_count, 12));

    for _ in 0..user_count {
        let uid = stream.read_u32()?;
        let edge_count = stream.read_u32()? as usize;
        let edges = stream.read_edges(edge_count)?;
        user_interactions.push((uid, edges));
    }

    // Section 3: Post Interactions
    let post_count = stream.read_u32()? as usize;
    let mut post_interactions = Vec::with_capacity(stream.bound_count(post_count, 8));

    for _ in 0..post_count {
        let pid = stream.read_u32()?;
        let edge_count = stream.read_u32()? as usize;
        let edges = stream.read_edges(edge_count)?;
        post_interactions.push((pid, edges));
    }

    // Section 4: Roaring Bitmaps
    let bm_user_count = stream.read_u32()? as usize;
    let mut user_likes_bitmaps = Vec::with_capacity(stream.bound_count(bm_user_count, 8));

    for _ in 0..bm_user_count {
        let uid = stream.read_u32()?;
        let len = stream.read_u32()? as usize;
        let bm_bytes = stream.read_bitmap(len)?;
        let bm = RoaringBitmap::deserialize_from(&bm_bytes[..]).map_err(|e| {
            FeedError::Snapshot(format!("RoaringBitmap deserialization failure: {e}"))
        })?;
        user_likes_bitmaps.push((uid, bm));
    }

    // Section 5: Follows
    let follower_count = stream.read_u32()? as usize;
    let mut follows = Vec::with_capacity(stream.bound_count(follower_count, 8));

    for _ in 0..follower_count {
        let fid = stream.read_u32()?;
        let count = stream.read_u32()? as usize;
        let list = stream.read_u32_vec(count)?;
        follows.push((fid, list));
    }

    // Section 6: Post Metadata
    let meta_count = stream.read_u32()? as usize;
    let mut post_metadata = Vec::with_capacity(stream.bound_count(meta_count, 27));
    let mut post_topics = Vec::new();

    for _ in 0..meta_count {
        let pid = stream.read_u32()?;
        let author_id = stream.read_u32()?;

        let has_root = stream.read_u8()? != 0;
        let root_val = stream.read_u32()?;
        let root_id = if has_root { Some(root_val) } else { None };

        let has_parent = stream.read_u8()? != 0;
        let parent_val = stream.read_u32()?;
        let parent_id = if has_parent { Some(parent_val) } else { None };

        let created_at = stream.read_u64()?;
        let (is_nsfw, topic_opt) = if version >= 5 {
            let raw_flags = stream.read_u8()?;
            (
                (raw_flags & 1) != 0,
                crate::graph::decode_topic_flag(raw_flags),
            )
        } else {
            (false, None)
        };
        if let Some(topic) = topic_opt {
            post_topics.push((pid, topic));
        }

        post_metadata.push((
            pid,
            PostMeta {
                author_id,
                root_id,
                parent_id,
                created_at,
                is_nsfw,
            },
        ));
    }

    // Section 7: Active Recent Posts
    let recent_count = stream.read_u32()? as usize;
    let mut active_recent_posts = Vec::with_capacity(stream.bound_count(recent_count, 12));

    for _ in 0..recent_count {
        let pid = stream.read_u32()?;
        let ts = stream.read_u64()?;
        active_recent_posts.push((pid, ts));
    }

    // Section 8: User Preferences (Version 2 / Version 3 / Version 4)
    let num_preferences = if (SNAPSHOT_FORMAT_VERSION_V2..=SNAPSHOT_FORMAT_VERSION)
        .contains(&version)
        && stream.has_more()?
    {
        let pref_count = stream.read_u32()? as usize;
        let mut user_preferences = Vec::with_capacity(stream.bound_count(pref_count, 50));

        for _ in 0..pref_count {
            let uid = stream.read_u32()?;
            let freshness = stream.read_f32()?;
            let serendipity = stream.read_f32()?;
            let art = stream.read_f32()?;
            let tech = stream.read_f32()?;
            let science = stream.read_f32()?;
            let news = stream.read_f32()?;
            let culture = stream.read_f32()?;
            let include_replies = if version >= 3 {
                stream.read_u8()? != 0
            } else {
                false
            };
            let min_likes = if version >= 4 {
                stream.read_u32()?
            } else {
                DEFAULT_MIN_LIKES
            };
            let no_nsfw = if version >= 5 {
                stream.read_u8()? != 0
            } else {
                false
            };
            let updated_at_secs = stream.read_u64()?;

            let dials = UserDials {
                freshness_half_life_secs: freshness,
                serendipity_ratio: serendipity,
                topic_weights: TopicWeights {
                    art,
                    tech,
                    science,
                    news,
                    culture,
                },
                include_replies,
                no_nsfw,
                min_likes,
                updated_at_secs,
            };

            if let Err(err) = dials.validate() {
                return Err(FeedError::Snapshot(format!(
                    "Corrupted user preference record in snapshot for user {uid}: {err}"
                )));
            }

            user_preferences.push((uid, dials));
        }

        preferences.restore_from_snapshot(user_preferences);
        u32::try_from(pref_count)
            .map_err(|e| FeedError::Snapshot(format!("Preference count exceeds u32: {e}")))?
    } else {
        preferences.clear();
        header_num_preferences
    };

    // Section 9 (optional): Impression History
    if stream.has_more()? {
        let imp_viewer_count = stream.read_u32()? as usize;
        let mut impression_records = Vec::with_capacity(stream.bound_count(imp_viewer_count, 8));
        for _ in 0..imp_viewer_count {
            let viewer_id = stream.read_u32()?;
            let entry_count = stream.read_u32()? as usize;
            let mut entries = Vec::with_capacity(stream.bound_count(entry_count, 16));
            for _ in 0..entry_count {
                let pid = stream.read_u32()?;
                let served_at_secs = stream.read_u64()?;
                let cnt = stream.read_u32()?;
                entries.push((pid, served_at_secs, cnt));
            }
            impression_records.push((viewer_id, entries));
        }
        impressions.restore_from_snapshot(impression_records);
    } else {
        impressions.clear();
    }

    // 4. Hydrate in-memory graph store (interner was hydrated directly in Section 1)
    graph.restore_from_snapshot(GraphSnapshotData {
        user_interactions,
        post_interactions,
        user_likes_bitmaps,
        follows,
        post_metadata,
        active_recent_posts,
    });
    for (pid, topic) in post_topics {
        graph.record_post_topic(pid, topic);
    }

    let load_duration_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    info!(
        "Hydrated snapshot in {load_duration_ms:.2} ms: {num_strings} strings, {num_users} users, {total_forward_edges} interactions, {num_preferences} preferences"
    );

    Ok(Some(LoadedSnapshot {
        header: SnapshotHeader {
            magic,
            format_version: version,
            header_length: HEADER_SIZE as u16,
            created_at_secs,
            jetstream_cursor_us,
            flags,
            num_strings,
            num_users,
            total_forward_edges,
            num_followers,
            num_post_metadata,
            payload_crc32: expected_payload_crc,
            header_crc32: expected_header_crc,
            num_preferences,
        },
        load_duration_ms,
    }))
}

/// Statistics recorded during memory store compaction.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionStats {
    /// Number of interned strings before compaction.
    pub strings_before: usize,
    /// Number of interned strings retained after compaction.
    pub strings_after: usize,
    /// Compaction duration in milliseconds.
    pub duration_ms: f64,
}

/// Compacts and garbage-collects all in-memory stores ([`StringInterner`], [`GraphStore`], [`UserPreferencesStore`]).
///
/// Discards all unreferenced strings and rebuilds graph structures with tightly packed allocations,
/// returning freed heap space to the memory allocator.
pub fn compact_memory_stores(
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
) -> CompactionStats {
    let start = Instant::now();
    let strings_before = interner.len();

    // 1. Collect all referenced string IDs
    let mut referenced = graph.collect_referenced_ids();
    preferences.collect_referenced_ids(&mut referenced);

    // 2. Compact StringInterner in-place shard-by-shard, dropping dead strings and maps immediately
    let id_map = interner.compact_referenced(&referenced);

    // 3. Compact graph data structures with remapped IDs shard-by-shard
    graph.compact(&id_map);

    // 4. Compact user preferences with remapped IDs
    preferences.remap(&id_map);

    let strings_after = interner.len();

    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
    CompactionStats {
        strings_before,
        strings_after,
        duration_ms,
    }
}

/// Compacts and garbage-collects all in-memory stores including [`crate::recommender::ImpressionStore`],
/// remapping viewer and post IDs across impressions so compaction never corrupts impression history.
pub fn compact_memory_stores_with_impressions(
    interner: &StringInterner,
    graph: &GraphStore,
    preferences: &UserPreferencesStore,
    impression_store: &crate::recommender::ImpressionStore,
) -> CompactionStats {
    let start = Instant::now();
    let strings_before = interner.len();

    // 1. Collect all referenced string IDs across graph, preferences, and impressions
    let mut referenced = graph.collect_referenced_ids();
    preferences.collect_referenced_ids(&mut referenced);
    referenced |= impression_store.collect_referenced_ids();

    // 2. Compact StringInterner in-place shard-by-shard, dropping dead strings and maps immediately
    let id_map = interner.compact_referenced(&referenced);

    // 3. Compact graph data structures with remapped IDs shard-by-shard
    graph.compact(&id_map);

    // 4. Compact user preferences with remapped IDs
    preferences.remap(&id_map);

    // 5. Compact impression store with remapped viewer & post IDs
    impression_store.compact(&id_map);

    let strings_after = interner.len();

    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
    CompactionStats {
        strings_before,
        strings_after,
        duration_ms,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::recommender::ImpressionStore;
    use crate::types::{SignalType, BLUESKY_EPOCH_SECS};

    #[test]
    fn test_snapshot_persists_and_restores_impression_store_and_compaction() {
        let snap_path = std::env::temp_dir().join(format!(
            "fyc_snap_with_impressions_{}_{}.bin",
            std::process::id(),
            123_456
        ));

        let interner = StringInterner::new();
        let graph = GraphStore::new();
        let prefs = UserPreferencesStore::new();
        let impressions = ImpressionStore::default();

        let viewer = interner.intern("did:plc:snap_viewer");
        let author = interner.intern("did:plc:snap_author");
        let post1 = interner.intern("at://did:plc:snap_author/app.bsky.feed.post/1");
        let post2 = interner.intern("at://did:plc:snap_author/app.bsky.feed.post/2");
        let _dead_string = interner.intern("did:plc:unreferenced_garbage");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_post_meta(post1, author, None, None, now - 500);
        graph.record_post_meta(post2, author, None, None, now - 400);
        graph.record_interaction(viewer, post1, SignalType::Like, now - 300);

        impressions.record_impressions(viewer, &[post1, post2], now - 60);
        impressions.record_impressions(viewer, &[post2], now - 30);

        // Compact with impressions before saving
        let stats = compact_memory_stores_with_impressions(&interner, &graph, &prefs, &impressions);
        assert!(stats.strings_after < stats.strings_before);

        let viewer_after = interner.lookup_id("did:plc:snap_viewer").unwrap();
        let post2_after = interner
            .lookup_id("at://did:plc:snap_author/app.bsky.feed.post/2")
            .unwrap();
        assert_eq!(impressions.get_viewer_impression_count(viewer_after), 2);
        assert!(
            impressions.contains_impression(viewer_after, post2_after),
            "Recently impressed post must remain tracked after compaction"
        );
        let penalty_before = impressions
            .evaluate_compounded_fatigue_penalty(viewer_after, post2_after, now)
            .unwrap();
        assert!(penalty_before < 0.25);

        save_snapshot_with_impressions(
            &snap_path,
            &interner,
            &graph,
            &prefs,
            &impressions,
            123_456,
        )
        .unwrap();

        let restored_interner = StringInterner::new();
        let restored_graph = GraphStore::new();
        let restored_prefs = UserPreferencesStore::new();
        let restored_impressions = ImpressionStore::default();

        let loaded = load_snapshot_with_impressions(
            &snap_path,
            &restored_interner,
            &restored_graph,
            &restored_prefs,
            &restored_impressions,
        )
        .unwrap()
        .unwrap();
        let _ = std::fs::remove_file(&snap_path);
        assert_eq!(loaded.header.jetstream_cursor_us, 123_456);

        let restored_viewer = restored_interner.lookup_id("did:plc:snap_viewer").unwrap();
        let restored_post1 = restored_interner
            .lookup_id("at://did:plc:snap_author/app.bsky.feed.post/1")
            .unwrap();
        let restored_post2 = restored_interner
            .lookup_id("at://did:plc:snap_author/app.bsky.feed.post/2")
            .unwrap();

        assert_eq!(
            restored_impressions.get_viewer_impression_count(restored_viewer),
            2
        );
        assert!(restored_impressions.contains_impression(restored_viewer, restored_post1));
        assert!(restored_impressions.contains_impression(restored_viewer, restored_post2));
        let penalty_after = restored_impressions
            .evaluate_compounded_fatigue_penalty(restored_viewer, restored_post2, now)
            .unwrap();
        assert!((penalty_after - penalty_before).abs() < 1e-6);
    }
}
