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
//! - **Section 6: Post Metadata**: `(post_id, author_id, root_id, parent_id, created_at)`.
//! - **Section 7: Active Recent Posts**: `(post_id, last_activity_timestamp)`.

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
use crate::interner::StringInterner;
use crate::preferences::UserPreferencesStore;
use crate::types::{CompactEdge, PostMeta, SnapshotStatusInfo, TopicWeights, UserDials};

/// Magic 4-byte header identifier: `b"FYFD"` (For-You Feed).
pub const SNAPSHOT_MAGIC: [u8; 4] = *b"FYFD";

/// Current snapshot format version (3 includes Section 8 User Preferences with `include_replies`).
pub const SNAPSHOT_FORMAT_VERSION: u16 = 3;

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

/// Zero-copy byte slice reader for ultra-fast binary deserialization.
struct ByteSliceReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ByteSliceReader<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        if self.offset >= self.data.len() {
            return Err(FeedError::Snapshot("Unexpected EOF reading u8".to_string()));
        }
        let b = self.data[self.offset];
        self.offset += 1;
        Ok(b)
    }

    fn read_u32(&mut self) -> Result<u32> {
        if self.offset.saturating_add(4) > self.data.len() {
            return Err(FeedError::Snapshot(
                "Unexpected EOF reading u32".to_string(),
            ));
        }
        let b: [u8; 4] = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
        ];
        self.offset += 4;
        Ok(u32::from_le_bytes(b))
    }

    fn read_u64(&mut self) -> Result<u64> {
        if self.offset.saturating_add(8) > self.data.len() {
            return Err(FeedError::Snapshot(
                "Unexpected EOF reading u64".to_string(),
            ));
        }
        let b: [u8; 8] = [
            self.data[self.offset],
            self.data[self.offset + 1],
            self.data[self.offset + 2],
            self.data[self.offset + 3],
            self.data[self.offset + 4],
            self.data[self.offset + 5],
            self.data[self.offset + 6],
            self.data[self.offset + 7],
        ];
        self.offset += 8;
        Ok(u64::from_le_bytes(b))
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset.saturating_add(len);
        if end > self.data.len() {
            return Err(FeedError::Snapshot(
                "Unexpected EOF reading byte slice".to_string(),
            ));
        }
        let slice = &self.data[self.offset..end];
        self.offset = end;
        Ok(slice)
    }
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

    // Export memory states
    let strings = interner.export_strings();
    let graph_data = graph.snapshot_data();
    let pref_data = preferences.snapshot_data();

    // Section 1: Strings
    let num_strings = u32::try_from(strings.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many strings for snapshot: {e}")))?;
    write_chunk(&num_strings.to_le_bytes())?;
    for s in &strings {
        let b = s.as_bytes();
        let str_len = u32::try_from(b.len())
            .map_err(|e| FeedError::Snapshot(format!("String length exceeds u32: {e}")))?;
        write_chunk(&str_len.to_le_bytes())?;
        write_chunk(b)?;
    }

    // Section 2: User Interactions (Forward)
    let num_users = u32::try_from(graph_data.user_interactions.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many users for snapshot: {e}")))?;
    write_chunk(&num_users.to_le_bytes())?;
    let mut total_forward_edges = 0u64;
    for (uid, edges) in &graph_data.user_interactions {
        write_chunk(&uid.to_le_bytes())?;
        let edge_count = u32::try_from(edges.len())
            .map_err(|e| FeedError::Snapshot(format!("Too many edges for user: {e}")))?;
        write_chunk(&edge_count.to_le_bytes())?;
        for e in edges {
            write_chunk(&e.target.to_le_bytes())?;
            write_chunk(&e.packed.to_le_bytes())?;
        }
        total_forward_edges = total_forward_edges.saturating_add(edges.len() as u64);
    }

    // Section 3: Post Interactions (Reverse)
    let num_posts = u32::try_from(graph_data.post_interactions.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many posts for snapshot: {e}")))?;
    write_chunk(&num_posts.to_le_bytes())?;
    for (pid, edges) in &graph_data.post_interactions {
        write_chunk(&pid.to_le_bytes())?;
        let edge_count = u32::try_from(edges.len())
            .map_err(|e| FeedError::Snapshot(format!("Too many edges for post: {e}")))?;
        write_chunk(&edge_count.to_le_bytes())?;
        for e in edges {
            write_chunk(&e.target.to_le_bytes())?;
            write_chunk(&e.packed.to_le_bytes())?;
        }
    }

    // Section 4: Roaring Bitmaps
    let num_bm_users = u32::try_from(graph_data.user_likes_bitmaps.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many bitmap users: {e}")))?;
    write_chunk(&num_bm_users.to_le_bytes())?;
    let mut bm_buf = Vec::new();
    for (uid, bm) in &graph_data.user_likes_bitmaps {
        bm_buf.clear();
        bm.serialize_into(&mut bm_buf)
            .map_err(|e| FeedError::Snapshot(format!("RoaringBitmap serialization error: {e}")))?;
        write_chunk(&uid.to_le_bytes())?;
        let bm_len = u32::try_from(bm_buf.len())
            .map_err(|e| FeedError::Snapshot(format!("Bitmap byte length exceeds u32: {e}")))?;
        write_chunk(&bm_len.to_le_bytes())?;
        write_chunk(&bm_buf)?;
    }

    // Section 5: Follows
    let num_followers = u32::try_from(graph_data.follows.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many followers: {e}")))?;
    write_chunk(&num_followers.to_le_bytes())?;
    for (fid, list) in &graph_data.follows {
        write_chunk(&fid.to_le_bytes())?;
        let count = u32::try_from(list.len())
            .map_err(|e| FeedError::Snapshot(format!("Too many followed users: {e}")))?;
        write_chunk(&count.to_le_bytes())?;
        for &target in list {
            write_chunk(&target.to_le_bytes())?;
        }
    }

    // Section 6: Post Metadata
    let num_post_metadata = u32::try_from(graph_data.post_metadata.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many metadata entries: {e}")))?;
    write_chunk(&num_post_metadata.to_le_bytes())?;
    for (pid, meta) in &graph_data.post_metadata {
        write_chunk(&pid.to_le_bytes())?;
        write_chunk(&meta.author_id.to_le_bytes())?;
        if let Some(r) = meta.root_id {
            write_chunk(&1u8.to_le_bytes())?;
            write_chunk(&r.to_le_bytes())?;
        } else {
            write_chunk(&0u8.to_le_bytes())?;
            write_chunk(&0u32.to_le_bytes())?;
        }
        if let Some(p) = meta.parent_id {
            write_chunk(&1u8.to_le_bytes())?;
            write_chunk(&p.to_le_bytes())?;
        } else {
            write_chunk(&0u8.to_le_bytes())?;
            write_chunk(&0u32.to_le_bytes())?;
        }
        write_chunk(&meta.created_at.to_le_bytes())?;
    }

    // Section 7: Active Recent Posts
    let num_recent = u32::try_from(graph_data.active_recent_posts.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many active recent posts: {e}")))?;
    write_chunk(&num_recent.to_le_bytes())?;
    for (pid, ts) in &graph_data.active_recent_posts {
        write_chunk(&pid.to_le_bytes())?;
        write_chunk(&ts.to_le_bytes())?;
    }

    // Section 8: User Preferences (Version 3)
    let num_preferences = u32::try_from(pref_data.len())
        .map_err(|e| FeedError::Snapshot(format!("Too many user preferences for snapshot: {e}")))?;
    write_chunk(&num_preferences.to_le_bytes())?;
    for (uid, dials) in &pref_data {
        write_chunk(&uid.to_le_bytes())?;
        write_chunk(&dials.freshness_half_life_secs.to_le_bytes())?;
        write_chunk(&dials.serendipity_ratio.to_le_bytes())?;
        write_chunk(&dials.topic_weights.art.to_le_bytes())?;
        write_chunk(&dials.topic_weights.tech.to_le_bytes())?;
        write_chunk(&dials.topic_weights.science.to_le_bytes())?;
        write_chunk(&dials.topic_weights.news.to_le_bytes())?;
        write_chunk(&dials.topic_weights.culture.to_le_bytes())?;
        write_chunk(&[u8::from(dials.include_replies)])?;
        write_chunk(&dials.updated_at_secs.to_le_bytes())?;
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

    // 2. Read entire payload and verify CRC32
    let payload_len = (metadata.len() as usize).saturating_sub(HEADER_SIZE);
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;

    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let actual_payload_crc = p_hasher.finalize();
    if actual_payload_crc != expected_payload_crc {
        return Err(FeedError::Snapshot(format!(
            "Payload CRC32 mismatch: expected {expected_payload_crc:#010x}, calculated {actual_payload_crc:#010x}"
        )));
    }

    // 3. Deserialize Payload from byte slice
    let mut slice_reader = ByteSliceReader::new(&payload);

    // Section 1: Strings
    let string_count = slice_reader.read_u32()? as usize;
    let mut interned_strings = Vec::with_capacity(string_count);

    for _ in 0..string_count {
        let len = slice_reader.read_u32()? as usize;
        let str_bytes = slice_reader.read_bytes(len)?;
        let s = std::str::from_utf8(str_bytes).map_err(|e| {
            FeedError::Snapshot(format!("Invalid UTF-8 in string interner snapshot: {e}"))
        })?;
        interned_strings.push(CompactString::new(s));
    }

    // Section 2: User Interactions
    let user_count = slice_reader.read_u32()? as usize;
    let mut user_interactions = Vec::with_capacity(user_count);

    for _ in 0..user_count {
        let uid = slice_reader.read_u32()?;
        let edge_count = slice_reader.read_u32()? as usize;
        let mut edges = Vec::with_capacity(edge_count);
        for _ in 0..edge_count {
            let target = slice_reader.read_u32()?;
            let packed = slice_reader.read_u32()?;
            edges.push(CompactEdge { target, packed });
        }
        user_interactions.push((uid, edges));
    }

    // Section 3: Post Interactions
    let post_count = slice_reader.read_u32()? as usize;
    let mut post_interactions = Vec::with_capacity(post_count);

    for _ in 0..post_count {
        let pid = slice_reader.read_u32()?;
        let edge_count = slice_reader.read_u32()? as usize;
        let mut edges = Vec::with_capacity(edge_count);
        for _ in 0..edge_count {
            let target = slice_reader.read_u32()?;
            let packed = slice_reader.read_u32()?;
            edges.push(CompactEdge { target, packed });
        }
        post_interactions.push((pid, edges));
    }

    // Section 4: Roaring Bitmaps
    let bm_user_count = slice_reader.read_u32()? as usize;
    let mut user_likes_bitmaps = Vec::with_capacity(bm_user_count);

    for _ in 0..bm_user_count {
        let uid = slice_reader.read_u32()?;
        let len = slice_reader.read_u32()? as usize;
        let bm_bytes = slice_reader.read_bytes(len)?;
        let bm = RoaringBitmap::deserialize_from(bm_bytes).map_err(|e| {
            FeedError::Snapshot(format!("RoaringBitmap deserialization failure: {e}"))
        })?;
        user_likes_bitmaps.push((uid, bm));
    }

    // Section 5: Follows
    let follower_count = slice_reader.read_u32()? as usize;
    let mut follows = Vec::with_capacity(follower_count);

    for _ in 0..follower_count {
        let fid = slice_reader.read_u32()?;
        let count = slice_reader.read_u32()? as usize;
        let mut list = Vec::with_capacity(count);
        for _ in 0..count {
            list.push(slice_reader.read_u32()?);
        }
        follows.push((fid, list));
    }

    // Section 6: Post Metadata
    let meta_count = slice_reader.read_u32()? as usize;
    let mut post_metadata = Vec::with_capacity(meta_count);

    for _ in 0..meta_count {
        let pid = slice_reader.read_u32()?;
        let author_id = slice_reader.read_u32()?;

        let has_root = slice_reader.read_u8()? != 0;
        let root_val = slice_reader.read_u32()?;
        let root_id = if has_root { Some(root_val) } else { None };

        let has_parent = slice_reader.read_u8()? != 0;
        let parent_val = slice_reader.read_u32()?;
        let parent_id = if has_parent { Some(parent_val) } else { None };

        let created_at = slice_reader.read_u64()?;

        post_metadata.push((
            pid,
            PostMeta {
                author_id,
                root_id,
                parent_id,
                created_at,
            },
        ));
    }

    // Section 7: Active Recent Posts
    let recent_count = slice_reader.read_u32()? as usize;
    let mut active_recent_posts = Vec::with_capacity(recent_count);

    for _ in 0..recent_count {
        let pid = slice_reader.read_u32()?;
        let ts = slice_reader.read_u64()?;
        active_recent_posts.push((pid, ts));
    }

    // Section 8: User Preferences (Version 2 / Version 3)
    let num_preferences = if (version == SNAPSHOT_FORMAT_VERSION
        || version == SNAPSHOT_FORMAT_VERSION_V2)
        && slice_reader.offset < payload.len()
    {
        let pref_count = slice_reader.read_u32()? as usize;
        let mut user_preferences = Vec::with_capacity(pref_count);

        for _ in 0..pref_count {
            let uid = slice_reader.read_u32()?;
            let freshness_bytes = slice_reader.read_bytes(4)?;
            let freshness = f32::from_le_bytes([
                freshness_bytes[0],
                freshness_bytes[1],
                freshness_bytes[2],
                freshness_bytes[3],
            ]);
            let serendipity_bytes = slice_reader.read_bytes(4)?;
            let serendipity = f32::from_le_bytes([
                serendipity_bytes[0],
                serendipity_bytes[1],
                serendipity_bytes[2],
                serendipity_bytes[3],
            ]);
            let art_bytes = slice_reader.read_bytes(4)?;
            let art = f32::from_le_bytes([art_bytes[0], art_bytes[1], art_bytes[2], art_bytes[3]]);
            let tech_bytes = slice_reader.read_bytes(4)?;
            let tech =
                f32::from_le_bytes([tech_bytes[0], tech_bytes[1], tech_bytes[2], tech_bytes[3]]);
            let science_bytes = slice_reader.read_bytes(4)?;
            let science = f32::from_le_bytes([
                science_bytes[0],
                science_bytes[1],
                science_bytes[2],
                science_bytes[3],
            ]);
            let news_bytes = slice_reader.read_bytes(4)?;
            let news =
                f32::from_le_bytes([news_bytes[0], news_bytes[1], news_bytes[2], news_bytes[3]]);
            let culture_bytes = slice_reader.read_bytes(4)?;
            let culture = f32::from_le_bytes([
                culture_bytes[0],
                culture_bytes[1],
                culture_bytes[2],
                culture_bytes[3],
            ]);
            let include_replies = if version >= 3 {
                slice_reader.read_u8()? != 0
            } else {
                false
            };
            let updated_at_secs = slice_reader.read_u64()?;

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

    // 4. Hydrate in-memory stores
    interner.hydrate_from(interned_strings);
    graph.restore_from_snapshot(GraphSnapshotData {
        user_interactions,
        post_interactions,
        user_likes_bitmaps,
        follows,
        post_metadata,
        active_recent_posts,
    });

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
