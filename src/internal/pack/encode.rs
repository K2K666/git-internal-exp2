//! Pack encoder capable of building streamed `.pack`/`.idx` pairs with optional delta compression,
//! windowing, and asynchronous writers.

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
};

use ahash::AHasher;
// use libc::ungetc;
use chrono::Utc;
use flate2::write::ZlibEncoder;
use natord::compare;
use rayon::prelude::*;
//use tokio::io::AsyncWriteExt;
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
use tokio::{fs::File, sync::mpsc, task::JoinHandle};

//use std::io as stdio;
use crate::delta;
use crate::{
    errors::GitError,
    hash::ObjectHash,
    internal::{
        metadata::{EntryMeta, MetaAttached},
        object::{
            ObjectTrait,
            tree::{Tree, TreeItemMode},
            types::ObjectType,
        },
        pack::{entry::Entry, index_entry::IndexEntry, pack_index::IdxBuilder},
    },
    time_it,
    utils::HashAlgorithm,
    zstdelta,
};

const MAX_CHAIN_LEN: usize = 50;
const MIN_DELTA_RATE: f64 = 0.5; // minimum delta rate
const PARALLEL_DELTA_WINDOW_THRESHOLD: usize = 32;
const BASENAME_DELTA_MIN_COUNT: usize = 3;
const BASENAME_DELTA_MIN_SIZE: usize = 256;
const BASENAME_DELTA_MIN_SIZE_RATIO: f64 = 0.35;
const BASENAME_DELTA_MAX_ANCHOR_SIZE: usize = 2 * 1024 * 1024;
//const MAX_ZSTDELTA_CHAIN_LEN: usize = 50;

/// A encoder for generating pack files with delta objects.
pub struct PackEncoder {
    //path: Option<PathBuf>,
    object_number: usize,
    process_index: usize,
    window_size: usize,
    // window: VecDeque<(Entry, usize)>, // entry and offset
    pack_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_sender: Option<mpsc::Sender<Vec<u8>>>,
    //idx_sender: Option<mpsc::Sender<Vec<u8>>>,
    idx_entries: Option<Vec<IndexEntry>>,
    inner_offset: usize,       // offset of current entry
    inner_hash: HashAlgorithm, // introduce different hash algorithm
    final_hash: Option<ObjectHash>,
    start_encoding: bool,
}

/// Encode entries into a pack, write `.pack`/`.idx` files to `output_dir`.
/// - Spawns background writers to consume pack/idx channels to avoid back-pressure.
/// - Uses `window_size` to control delta: `0` means no delta (parallel encode), otherwise enable delta window.
/// # Arguments
/// * `raw_entries_rx` - receiver providing entries with metadata
/// * `object_number` - expected total object count for the pack header
/// * `output_dir` - target directory to place the generated files
/// * `window_size` - delta window size; `0` disables delta
/// # Returns
/// * `Ok(())` on success, `GitError` on failure
pub async fn encode_and_output_to_files(
    raw_entries_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    object_number: usize,
    output_dir: PathBuf,
    window_size: usize,
) -> Result<(), GitError> {
    let (pack_tx, mut pack_rx) = mpsc::channel(1024);
    let (idx_tx, mut idx_rx) = mpsc::channel(1024);
    let mut pack_encoder = PackEncoder::new_with_idx(object_number, window_size, pack_tx, idx_tx);

    // timestamp for temp filename
    let now = Utc::now();
    let timestamp = now.format("%Y%m%d%H%M%S%.3f").to_string(); // 例如 20251209235959.123
    let tmp_path = output_dir.join(format!("{}objects.pack.tmp", timestamp));
    let mut pack_file = File::create(&tmp_path).await?;

    let pack_writer = tokio::spawn(async move {
        while let Some(chunk) = pack_rx.recv().await {
            TokioAsyncWriteExt::write_all(&mut pack_file, &chunk).await?;
        }
        //pack_file.flush().await?;
        TokioAsyncWriteExt::flush(&mut pack_file).await?;
        Ok::<(), GitError>(())
    });

    pack_encoder.encode(raw_entries_rx).await?;

    // 等待 pack 写入完成
    let pack_write_result = pack_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("pack writer task join error: {e}")))?;
    pack_write_result?;

    let final_pack_name =
        output_dir.join(format!("pack-{}.pack", pack_encoder.final_hash.unwrap()));
    let final_idx_name = output_dir.join(format!("pack-{}.idx", pack_encoder.final_hash.unwrap()));
    tokio::fs::rename(tmp_path, &final_pack_name).await?;

    let mut idx_file = File::create(&final_idx_name).await?;
    let idx_writer = tokio::spawn(async move {
        while let Some(chunk) = idx_rx.recv().await {
            //idx_file.write_all(&chunk).await?;
            TokioAsyncWriteExt::write_all(&mut idx_file, &chunk).await?;
        }
        //idx_file.flush().await?;
        TokioAsyncWriteExt::flush(&mut idx_file).await?;
        Ok::<(), GitError>(())
    });

    //build idx
    pack_encoder.encode_idx_file().await?;

    let idx_write_result = idx_writer
        .await
        .map_err(|e| GitError::PackEncodeError(format!("idx writer task join error: {e}")))?;
    idx_write_result?;

    Ok(())
}

/// Encode header of pack file (12 byte)<br>
/// Content: 'PACK', Version(2), number of objects
fn encode_header(object_number: usize) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::with_capacity(12);
    result.extend_from_slice(&[
        b'P', b'A', b'C', b'K', // The logotype of the Pack File
        0, 0, 0, 2, // generates version 2 only.
    ]);
    assert_ne!(object_number, 0); // guarantee self.number_of_objects!=0
    assert!(object_number <= u32::MAX as usize);
    //TODO: GitError:numbers of objects should < 4G ,
    result.extend_from_slice(&(object_number as u32).to_be_bytes()); // to 4 bytes (network byte order aka. big-endian)
    result
}

/// Encode offset of delta object
fn encode_offset(mut value: usize) -> Vec<u8> {
    assert_ne!(value, 0, "offset can't be zero");
    let mut bytes = Vec::with_capacity(std::mem::size_of::<usize>() + 1);

    bytes.push((value & 0x7F) as u8);
    value >>= 7;
    while value != 0 {
        value -= 1;
        let byte = (value & 0x7F) as u8 | 0x80; // set first bit one
        value >>= 7;
        bytes.push(byte);
    }
    bytes.reverse();
    bytes
}

/// Encode one object, and update the hash
/// @offset: offset of this object if it's a delta object. For other object, it's None
fn encode_one_object(entry: &Entry, offset: Option<usize>) -> Result<Vec<u8>, GitError> {
    // try encode as delta
    let obj_data = &entry.data;
    let obj_data_len = obj_data.len();
    let obj_type_number = entry.obj_type.to_pack_type_u8()?;

    let mut encoded_data = Vec::with_capacity(obj_data_len + 16);

    // **header** encoding
    let mut first_byte = (obj_type_number << 4) | (obj_data_len & 0x0f) as u8;
    let mut size = obj_data_len >> 4; // 4 bit has been used in first byte
    if size != 0 {
        first_byte |= 0x80;
    }
    encoded_data.push(first_byte);

    while size != 0 {
        let mut byte = (size & 0x7f) as u8;
        size >>= 7;
        if size != 0 {
            byte |= 0x80;
        }
        encoded_data.push(byte);
    }

    // **offset** encoding
    if entry.obj_type == ObjectType::OffsetDelta || entry.obj_type == ObjectType::OffsetZstdelta {
        let offset_data = encode_offset(offset.unwrap());
        encoded_data.extend(offset_data);
    } else if entry.obj_type == ObjectType::HashDelta {
        unreachable!("unsupported type")
    }

    // **data** encoding, need zlib compress
    let mut inflate = ZlibEncoder::new(Vec::new(), flate2::Compression::best());
    inflate
        .write_all(obj_data)
        .expect("zlib compress should never failed");
    let compressed_data = inflate.finish().expect("zlib compress should never failed");
    encoded_data.extend(compressed_data);
    Ok(encoded_data)
}

/// Magic sort function for entries
fn magic_sort(a: &MetaAttached<Entry, EntryMeta>, b: &MetaAttached<Entry, EntryMeta>) -> Ordering {
    let path_a = a.meta.file_path.as_ref();
    let path_b = b.meta.file_path.as_ref();

    // 1. Handle path existence: entries with paths sort first
    match (path_a, path_b) {
        (Some(pa), Some(pb)) => {
            let pa = Path::new(pa);
            let pb = Path::new(pb);

            // 1. Compare parent directory paths
            let dir_ord = pa.parent().cmp(&pb.parent());
            if dir_ord != Ordering::Equal {
                return dir_ord;
            }

            // 2. Compare filenames (natural sort)
            let name_a = pa.file_name().unwrap_or_default().to_string_lossy();
            let name_b = pb.file_name().unwrap_or_default().to_string_lossy();
            let name_ord = compare(&name_a, &name_b);
            if name_ord != Ordering::Equal {
                return name_ord;
            }
        }
        (Some(_), None) => return Ordering::Less, // entries with paths sort first
        (None, Some(_)) => return Ordering::Greater, // entries without paths sort last
        (None, None) => {}
    }

    let ord = b.inner.data.len().cmp(&a.inner.data.len());
    if ord != Ordering::Equal {
        return ord;
    }

    // fallback pointer order (newest first)
    (a as *const MetaAttached<Entry, EntryMeta>).cmp(&(b as *const MetaAttached<Entry, EntryMeta>))
}

/// Calculate hash of data
fn calc_hash(data: &[u8]) -> u64 {
    let mut hasher = AHasher::default();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Cheap check if two byte slices are similar by comparing sampled chunks.
fn cheap_similar(a: &[u8], b: &[u8]) -> bool {
    let min_len = a.len().min(b.len());
    if min_len == 0 {
        return false;
    }
    if min_len <= 32 {
        return a == b;
    }

    let sample_len = min_len.min(64);
    let max_start = min_len - sample_len;
    let starts = [
        0,
        max_start / 4,
        max_start / 2,
        max_start.saturating_mul(3) / 4,
        max_start,
    ];

    let mut matches = 0;
    let mut last_start = None;
    let mut unique_samples = 0;
    for start in starts {
        if last_start == Some(start) {
            continue;
        }
        last_start = Some(start);
        unique_samples += 1;
        if calc_hash(&a[start..start + sample_len]) == calc_hash(&b[start..start + sample_len]) {
            matches += 1;
        }
    }

    matches > 0 && (min_len < 512 || matches * 2 >= unique_samples)
}

fn score_delta_candidate(base: &Entry, entry: &Entry) -> Option<f64> {
    if base.obj_type != entry.obj_type {
        return None;
    }

    if base.chain_len >= MAX_CHAIN_LEN {
        return None;
    }

    if base.hash == entry.hash {
        return None;
    }

    let sym_ratio = (base.data.len().min(entry.data.len()) as f64)
        / (base.data.len().max(entry.data.len()) as f64);
    if sym_ratio < 0.5 {
        return None;
    }

    if !cheap_similar(&base.data, &entry.data) {
        return None;
    }

    let rate = if (base.data.len() + entry.data.len()) / 2 > 64 {
        delta::heuristic_encode_rate_parallel(&base.data, &entry.data)
    } else {
        delta::encode_rate(&base.data, &entry.data)
    };

    (rate > MIN_DELTA_RATE).then_some(rate)
}

fn same_blob_path(
    base: &MetaAttached<Entry, EntryMeta>,
    entry: &MetaAttached<Entry, EntryMeta>,
) -> bool {
    base.meta.file_path.is_some() && base.meta.file_path == entry.meta.file_path
}

fn blob_basename(entry: &MetaAttached<Entry, EntryMeta>) -> Option<String> {
    let path = entry.meta.file_path.as_ref()?;
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

fn can_try_basename_zstdelta(
    base: &MetaAttached<Entry, EntryMeta>,
    entry: &MetaAttached<Entry, EntryMeta>,
) -> bool {
    if same_blob_path(base, entry)
        || base.inner.chain_len >= MAX_CHAIN_LEN
        || base.inner.hash == entry.inner.hash
    {
        return false;
    }

    let min_len = base.inner.data.len().min(entry.inner.data.len());
    if min_len < BASENAME_DELTA_MIN_SIZE {
        return false;
    }

    let sym_ratio = min_len as f64 / base.inner.data.len().max(entry.inner.data.len()) as f64;
    sym_ratio >= BASENAME_DELTA_MIN_SIZE_RATIO
}

fn infer_blob_paths(
    commits: &[MetaAttached<Entry, EntryMeta>],
    trees: &[MetaAttached<Entry, EntryMeta>],
) -> HashMap<ObjectHash, String> {
    let parsed_trees = trees
        .iter()
        .filter_map(|entry| {
            Tree::from_bytes(&entry.inner.data, entry.inner.hash)
                .ok()
                .map(|tree| (entry.inner.hash, tree))
        })
        .collect::<HashMap<_, _>>();
    if parsed_trees.is_empty() {
        return HashMap::new();
    }

    let mut blob_paths = HashMap::new();
    let mut queue = commits
        .iter()
        .filter_map(|entry| commit_root_tree(&entry.inner.data))
        .filter(|hash| parsed_trees.contains_key(hash))
        .map(|hash| (hash, String::new()))
        .collect::<VecDeque<_>>();

    if queue.is_empty() {
        queue.extend(
            parsed_trees
                .keys()
                .copied()
                .map(|hash| (hash, String::new())),
        );
    }

    let mut visited = HashSet::new();
    while let Some((tree_hash, prefix)) = queue.pop_front() {
        if !visited.insert(tree_hash) {
            continue;
        }

        let Some(tree) = parsed_trees.get(&tree_hash) else {
            continue;
        };

        for item in &tree.tree_items {
            let path = join_git_path(&prefix, &item.name);
            match item.mode {
                TreeItemMode::Tree => {
                    if parsed_trees.contains_key(&item.id) {
                        queue.push_back((item.id, path));
                    }
                }
                TreeItemMode::Blob | TreeItemMode::BlobExecutable | TreeItemMode::Link => {
                    blob_paths.entry(item.id).or_insert(path);
                }
                TreeItemMode::Commit => {}
            }
        }
    }

    for tree in parsed_trees.values() {
        for item in &tree.tree_items {
            if matches!(
                item.mode,
                TreeItemMode::Blob | TreeItemMode::BlobExecutable | TreeItemMode::Link
            ) {
                blob_paths
                    .entry(item.id)
                    .or_insert_with(|| item.name.clone());
            }
        }
    }

    blob_paths
}

fn commit_root_tree(data: &[u8]) -> Option<ObjectHash> {
    let line = data.split(|byte| *byte == b'\n').next()?;
    let hex_hash = line.strip_prefix(b"tree ")?;
    let hex_hash = std::str::from_utf8(hex_hash).ok()?;
    ObjectHash::from_str(hex_hash).ok()
}

fn join_git_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}/{name}")
    }
}

fn attach_inferred_blob_paths(
    commits: &[MetaAttached<Entry, EntryMeta>],
    trees: &[MetaAttached<Entry, EntryMeta>],
    blobs: &mut [MetaAttached<Entry, EntryMeta>],
) {
    let blob_paths = infer_blob_paths(commits, trees);
    if blob_paths.is_empty() {
        return;
    }

    for blob in blobs {
        if blob.meta.file_path.is_none()
            && let Some(path) = blob_paths.get(&blob.inner.hash)
        {
            blob.meta.file_path = Some(path.clone());
        }
    }
}

fn candidate_is_better(rate: f64, base: &Entry, best_rate: f64, best_base: &Entry) -> bool {
    let tie_epsilon: f64 = 0.15;
    if rate > best_rate + tie_epsilon {
        true
    } else if (rate - best_rate).abs() <= tie_epsilon {
        base.chain_len < best_base.chain_len
    } else {
        false
    }
}

impl PackEncoder {
    pub fn new(object_number: usize, window_size: usize, sender: mpsc::Sender<Vec<u8>>) -> Self {
        PackEncoder {
            object_number,
            window_size,
            process_index: 0,
            // window: VecDeque::with_capacity(window_size),
            pack_sender: Some(sender),
            idx_sender: None,
            idx_entries: None,
            inner_offset: 12, // start  after 12 bytes pack header(signature + version + object count).
            inner_hash: HashAlgorithm::new(), // introduce different hash algorithm
            final_hash: None,
            start_encoding: false,
        }
    }

    pub fn new_with_idx(
        object_number: usize,
        window_size: usize,
        pack_sender: mpsc::Sender<Vec<u8>>,
        idx_sender: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        PackEncoder {
            //path: Some(path),
            object_number,
            window_size,
            process_index: 0,
            // window: VecDeque::with_capacity(window_size),
            pack_sender: Some(pack_sender),
            idx_sender: Some(idx_sender),
            idx_entries: None,
            inner_offset: 12, // start  after 12 bytes pack header(signature + version + object count).
            inner_hash: HashAlgorithm::new(), // introduce different hash algorithm
            final_hash: None,
            start_encoding: false,
        }
    }

    pub fn drop_sender(&mut self) {
        self.pack_sender.take(); // Take the sender out, dropping it
    }

    pub async fn send_data(&mut self, data: Vec<u8>) {
        if let Some(sender) = &self.pack_sender {
            sender.send(data).await.unwrap();
        }
    }

    /// Get the hash of the pack file. if the pack file is not finished, return None
    pub fn get_hash(&self) -> Option<ObjectHash> {
        self.final_hash
    }

    /// Encodes entries into a pack file with delta objects and outputs them through the specified writer.
    /// # Arguments
    /// - `rx` - A receiver channel (`mpsc::Receiver<Entry>`) from which entries to be encoded are received.
    /// # Returns
    /// Returns `Ok(())` if encoding is successful, or a `GitError` in case of failure.
    /// - Returns a `GitError` if there is a failure during the encoding process.
    /// - Returns `PackEncodeError` if an encoding operation is already in progress.
    pub async fn encode(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        if self.window_size == 0 {
            self.parallel_encode(entry_rx).await
        } else {
            self.inner_encode(entry_rx, true).await
        }
    }

    /// Encode with zstdelta
    pub async fn encode_with_zstdelta(
        &mut self,
        entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        self.inner_encode(entry_rx, true).await
    }

    /// Delta selection heuristics are based on:
    ///   https://github.com/git/git/blob/master/Documentation/technical/pack-heuristics.adoc
    async fn inner_encode(
        &mut self,
        mut entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
        enable_zstdelta: bool,
    ) -> Result<(), GitError> {
        // ensure only one decode can only invoke once
        if self.start_encoding {
            return Err(GitError::PackEncodeError(
                "encoding operation is already in progress".to_string(),
            ));
        }
        self.start_encoding = true;

        let head = encode_header(self.object_number);
        self.send_data(head.clone()).await;
        self.inner_hash.update(&head);

        let mut commits: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut trees: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut blobs: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        let mut tags: Vec<MetaAttached<Entry, EntryMeta>> = Vec::new();
        while let Some(entry) = entry_rx.recv().await {
            match entry.inner.obj_type {
                ObjectType::Commit => {
                    commits.push(entry);
                }
                ObjectType::Tree => {
                    trees.push(entry);
                }
                ObjectType::Blob => {
                    blobs.push(entry);
                }
                ObjectType::Tag => {
                    tags.push(entry);
                }
                _ => {
                    return Err(GitError::PackEncodeError(format!(
                        "object type `{}` is not supported by delta-window pack encoding",
                        entry.inner.obj_type
                    )));
                }
            }
            self.process_index += 1;
        }

        attach_inferred_blob_paths(&commits, &trees, &mut blobs);
        commits.sort_by(magic_sort);
        trees.sort_by(magic_sort);
        blobs.sort_by(magic_sort);
        tags.sort_by(magic_sort);
        tracing::info!(
            "numbers :  commits: {:?} trees: {:?} blobs:{:?} tag :{:?}",
            commits.len(),
            trees.len(),
            blobs.len(),
            tags.len()
        );

        // parallel encoding vec with different object_type
        let window_size = self.window_size;
        let (commit_results, tree_results, blob_results, tag_results) = tokio::try_join!(
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    commits
                        .into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    window_size,
                    enable_zstdelta,
                )
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    trees
                        .into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    window_size,
                    enable_zstdelta,
                )
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_blobs_as_offset_delta(blobs, window_size, enable_zstdelta)
            }),
            tokio::task::spawn_blocking(move || {
                Self::try_as_offset_delta(
                    tags.into_iter()
                        .map(|entry_with_meta| entry_with_meta.inner)
                        .collect(),
                    window_size,
                    enable_zstdelta,
                )
            }),
        )
        .map_err(|e| GitError::PackEncodeError(format!("Task join error: {e}")))?;

        let commit_res = commit_results?;
        let tree_res = tree_results?;
        let blob_res = blob_results?;
        let tag_res = tag_results?;

        let mut idx_entries = Vec::with_capacity(self.object_number);
        for res in [commit_res, tree_res, blob_res, tag_res] {
            for (obj_data, mut idx_entry) in res {
                idx_entry.offset = self.inner_offset as u64;
                self.write_vec_and_update(obj_data).await;
                idx_entries.push(idx_entry);
            }
        }

        self.idx_entries = Some(idx_entries);

        if self.process_index != self.object_number {
            return Err(GitError::PackEncodeError(format!(
                "not all objects are encoded, process:{}, total:{}",
                self.process_index, self.object_number
            )));
        }

        // Hash signature
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;

        self.drop_sender();
        Ok(())
    }

    /// Try to encode as delta using objects in window
    /// delta & zstdelta have been gathered here
    /// Refs: https://sapling-scm.com/docs/dev/internals/zstdelta/
    /// the sliding window was moved here
    /// # Returns
    /// - Return (Vec<Vec<u8>) if success make delta
    /// - Return (None) if didn't delta,
    fn try_as_offset_delta(
        mut bucket: Vec<Entry>,
        window_size: usize,
        enable_zstdelta: bool,
    ) -> Result<Vec<(Vec<u8>, IndexEntry)>, GitError> {
        let mut current_offset = 0usize;
        let mut window: VecDeque<(Entry, usize)> = VecDeque::with_capacity(window_size);
        let mut res: Vec<(Vec<u8>, IndexEntry)> = Vec::with_capacity(bucket.len());
        //let mut idx_entries: Vec<IndexEntry> = Vec::new();

        for entry in bucket.iter_mut() {
            //let entry_for_window = entry.clone();
            // 每次循环重置最佳基对象选择
            let mut best_base: Option<&(Entry, usize)> = None;
            let mut best_rate: f64 = 0.0;
            let candidates: Vec<_> = if window.len() >= PARALLEL_DELTA_WINDOW_THRESHOLD {
                window
                    .par_iter()
                    .filter_map(|try_base| {
                        score_delta_candidate(&try_base.0, entry).map(|rate| (rate, try_base))
                    })
                    .collect()
            } else {
                window
                    .iter()
                    .filter_map(|try_base| {
                        score_delta_candidate(&try_base.0, entry).map(|rate| (rate, try_base))
                    })
                    .collect()
            };

            for (rate, try_base) in candidates {
                match best_base {
                    None => {
                        best_rate = rate;
                        //best_base_offset = current_offset - try_base.1;
                        best_base = Some(try_base);
                    }
                    Some(best_base_ref) => {
                        if candidate_is_better(rate, &try_base.0, best_rate, &best_base_ref.0) {
                            best_rate = rate;
                            best_base = Some(try_base);
                        }
                    }
                }
            }

            let mut entry_for_window = entry.clone();

            let offset = best_base.map(|best_base| {
                let delta = if enable_zstdelta {
                    entry.obj_type = ObjectType::OffsetZstdelta;
                    zstdelta::diff(&best_base.0.data, &entry.data)
                        .map_err(|e| {
                            GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                        })
                        .unwrap()
                } else {
                    entry.obj_type = ObjectType::OffsetDelta;
                    delta::encode(&best_base.0.data, &entry.data)
                };
                //entry.obj_type = ObjectType::OffsetDelta;
                entry.data = delta;
                entry.chain_len = best_base.0.chain_len + 1;
                current_offset - best_base.1
            });

            entry_for_window.chain_len = entry.chain_len;
            let obj_data = encode_one_object(entry, offset)?;
            window.push_back((entry_for_window, current_offset));
            if window.len() > window_size {
                window.pop_front();
            }
            let obj_len = obj_data.len();
            res.push((obj_data, IndexEntry::new(entry, 0)));
            current_offset += obj_len;
        }
        Ok(res)
    }

    fn try_blobs_as_offset_delta(
        mut bucket: Vec<MetaAttached<Entry, EntryMeta>>,
        window_size: usize,
        enable_zstdelta: bool,
    ) -> Result<Vec<(Vec<u8>, IndexEntry)>, GitError> {
        let mut current_offset = 0usize;
        let mut window: VecDeque<(MetaAttached<Entry, EntryMeta>, usize)> =
            VecDeque::with_capacity(window_size);
        let mut res: Vec<(Vec<u8>, IndexEntry)> = Vec::with_capacity(bucket.len());
        let mut basename_counts: HashMap<String, usize> = HashMap::new();
        for entry in &bucket {
            if let Some(name) = blob_basename(entry) {
                *basename_counts.entry(name).or_default() += 1;
            }
        }
        let mut basename_anchors: HashMap<String, (MetaAttached<Entry, EntryMeta>, usize)> =
            HashMap::new();

        for entry_with_meta in bucket.iter_mut() {
            let mut forced_delta = None;
            let basename = blob_basename(entry_with_meta).filter(|name| {
                basename_counts
                    .get(name)
                    .is_some_and(|count| *count >= BASENAME_DELTA_MIN_COUNT)
            });
            if enable_zstdelta {
                for base in window.iter().rev().filter(|base| {
                    same_blob_path(&base.0, entry_with_meta)
                        && base.0.inner.chain_len < MAX_CHAIN_LEN
                        && base.0.inner.hash != entry_with_meta.inner.hash
                }) {
                    let delta = zstdelta::diff(&base.0.inner.data, &entry_with_meta.inner.data)
                        .map_err(|e| {
                            GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                        })?;
                    if delta.len() < entry_with_meta.inner.data.len()
                        && forced_delta.as_ref().is_none_or(
                            |(_, _, best_delta): &(usize, usize, Vec<u8>)| {
                                delta.len() < best_delta.len()
                            },
                        )
                    {
                        forced_delta =
                            Some((base.0.inner.chain_len + 1, current_offset - base.1, delta));
                    }
                }

                if let Some((base, base_offset)) = basename
                    .as_ref()
                    .and_then(|name| basename_anchors.get(name))
                    .filter(|(base, _)| can_try_basename_zstdelta(base, entry_with_meta))
                {
                    let delta = zstdelta::diff(&base.inner.data, &entry_with_meta.inner.data)
                        .map_err(|e| {
                            GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                        })?;
                    if delta.len() < entry_with_meta.inner.data.len()
                        && forced_delta.as_ref().is_none_or(
                            |(_, _, best_delta): &(usize, usize, Vec<u8>)| {
                                delta.len() < best_delta.len()
                            },
                        )
                    {
                        forced_delta = Some((
                            base.inner.chain_len + 1,
                            current_offset - *base_offset,
                            delta,
                        ));
                    }
                }
            }

            let mut best_base: Option<&(MetaAttached<Entry, EntryMeta>, usize)> = None;
            let mut best_rate: f64 = 0.0;
            let mut best_delta: Option<Vec<u8>> = None;
            if forced_delta.is_none() {
                let candidates: Vec<_> = if window.len() >= PARALLEL_DELTA_WINDOW_THRESHOLD {
                    window
                        .par_iter()
                        .filter_map(|try_base| {
                            score_delta_candidate(&try_base.0.inner, &entry_with_meta.inner)
                                .map(|rate| (rate, try_base))
                        })
                        .collect()
                } else {
                    window
                        .iter()
                        .filter_map(|try_base| {
                            score_delta_candidate(&try_base.0.inner, &entry_with_meta.inner)
                                .map(|rate| (rate, try_base))
                        })
                        .collect()
                };

                if enable_zstdelta {
                    for (_, try_base) in candidates {
                        let delta =
                            zstdelta::diff(&try_base.0.inner.data, &entry_with_meta.inner.data)
                                .map_err(|e| {
                                    GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                                })?;
                        if delta.len() < entry_with_meta.inner.data.len()
                            && best_delta
                                .as_ref()
                                .is_none_or(|best_delta| delta.len() < best_delta.len())
                        {
                            best_base = Some(try_base);
                            best_delta = Some(delta);
                        }
                    }
                } else {
                    for (rate, try_base) in candidates {
                        match best_base {
                            None => {
                                best_rate = rate;
                                best_base = Some(try_base);
                            }
                            Some(best_base_ref) => {
                                if candidate_is_better(
                                    rate,
                                    &try_base.0.inner,
                                    best_rate,
                                    &best_base_ref.0.inner,
                                ) {
                                    best_rate = rate;
                                    best_base = Some(try_base);
                                }
                            }
                        }
                    }
                }
            }

            let mut entry_for_window = entry_with_meta.clone();
            let entry = &mut entry_with_meta.inner;

            let offset = if let Some((chain_len, offset, delta)) = forced_delta {
                entry.obj_type = ObjectType::OffsetZstdelta;
                entry.data = delta;
                entry.chain_len = chain_len;
                Some(offset)
            } else {
                best_base.map(|best_base| {
                    let delta = if enable_zstdelta {
                        entry.obj_type = ObjectType::OffsetZstdelta;
                        best_delta.take().unwrap_or_else(|| {
                            zstdelta::diff(&best_base.0.inner.data, &entry.data)
                                .map_err(|e| {
                                    GitError::DeltaObjectError(format!("zstdelta diff failed: {e}"))
                                })
                                .unwrap()
                        })
                    } else {
                        entry.obj_type = ObjectType::OffsetDelta;
                        delta::encode(&best_base.0.inner.data, &entry.data)
                    };
                    entry.data = delta;
                    entry.chain_len = best_base.0.inner.chain_len + 1;
                    current_offset - best_base.1
                })
            };

            entry_for_window.inner.chain_len = entry.chain_len;
            let obj_data = encode_one_object(entry, offset)?;
            if let Some(name) = basename
                && entry_for_window.inner.data.len() <= BASENAME_DELTA_MAX_ANCHOR_SIZE
            {
                basename_anchors
                    .entry(name)
                    .and_modify(|(base, offset)| {
                        if entry_for_window.inner.data.len() > base.inner.data.len() {
                            *base = entry_for_window.clone();
                            *offset = current_offset;
                        }
                    })
                    .or_insert_with(|| (entry_for_window.clone(), current_offset));
            }
            window.push_back((entry_for_window, current_offset));
            if window.len() > window_size {
                window.pop_front();
            }
            let obj_len = obj_data.len();
            res.push((obj_data, IndexEntry::new(entry, 0)));
            current_offset += obj_len;
        }
        Ok(res)
    }

    /// Parallel encode with rayon, only works when window_size == 0 (no delta)
    pub async fn parallel_encode(
        &mut self,
        mut entry_rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<(), GitError> {
        if self.window_size != 0 {
            return Err(GitError::PackEncodeError(
                "parallel encode only works when window_size == 0".to_string(),
            ));
        }

        // ensure only one decode can only invoke once
        if self.start_encoding {
            return Err(GitError::PackEncodeError(
                "encoding operation is already in progress".to_string(),
            ));
        }
        self.start_encoding = true;

        let head = encode_header(self.object_number);
        self.send_data(head.clone()).await;
        self.inner_hash.update(&head);

        let mut idx_entries = Vec::with_capacity(self.object_number);
        let batch_size = usize::max(1000, entry_rx.max_capacity() / 10); // A temporary value, not optimized
        tracing::info!("encode with batch size: {}", batch_size);
        loop {
            let mut batch_entries = Vec::with_capacity(batch_size);
            time_it!("parallel encode: receive batch", {
                for _ in 0..batch_size {
                    match entry_rx.recv().await {
                        Some(entry) => {
                            if entry.inner.obj_type.is_ai_object() {
                                return Err(GitError::PackEncodeError(format!(
                                    "AI object type `{}` cannot be encoded in a pack file",
                                    entry.inner.obj_type
                                )));
                            }
                            batch_entries.push(entry.inner);
                            self.process_index += 1;
                        }
                        None => break,
                    }
                }
            });

            if batch_entries.is_empty() {
                break;
            }

            // use `collect` will return result in order, refs: https://github.com/rayon-rs/rayon/issues/551#issuecomment-371657900
            let batch_result: Vec<Result<(Vec<u8>, IndexEntry), GitError>> =
                time_it!("parallel encode: encode batch", {
                    batch_entries
                        .par_iter()
                        .map(|entry| {
                            encode_one_object(entry, None)
                                .map(|encoded| (encoded, IndexEntry::new(entry, 0)))
                        })
                        .collect()
                });

            time_it!("parallel encode: write batch", {
                for obj_data in batch_result {
                    let (encoded, mut idx_entry) = obj_data?;
                    idx_entry.offset = self.inner_offset as u64;
                    self.write_vec_and_update(encoded).await;
                    idx_entries.push(idx_entry);
                }
            });
        }

        tracing::debug!("parallel encode idx entries: {:?}", idx_entries.len());
        if self.process_index != self.object_number {
            panic!(
                "not all objects are encoded, process:{}, total:{}",
                self.process_index, self.object_number
            );
        }

        // hash signature
        let hash_result = self.inner_hash.clone().finalize();
        self.final_hash = Some(ObjectHash::from_bytes(&hash_result).unwrap());
        self.send_data(hash_result.to_vec()).await;
        self.drop_sender();

        self.idx_entries = Some(idx_entries);
        Ok(())
    }

    async fn write_vec_and_update(&mut self, data: Vec<u8>) {
        self.inner_hash.update(&data);
        self.inner_offset += data.len();
        self.send_data(data).await;
    }

    async fn generate_idx_file(&mut self) -> Result<(), GitError> {
        let final_hash = self.final_hash
            .ok_or(GitError::PackEncodeError("final_hash is missing,The pack file must be generated before the index file is produced.".into()))?;
        let idx_entries = self.idx_entries.clone().ok_or(GitError::PackEncodeError(
            "The pack file must be generated before the index file is produced.".into(),
        ))?;
        let mut idx_builder = IdxBuilder::new(
            self.object_number,
            self.idx_sender.clone().unwrap(),
            final_hash,
        );
        idx_builder.write_idx(idx_entries).await?;
        Ok(())
    }

    /// async version of encode, result data will be returned by JoinHandle.
    /// It will consume PackEncoder, so you can't use it after calling this function.
    /// when window_size = 0, it executes parallel_encode which retains stream transmission
    /// when window_size = 0,it executes encode which uses magic sort and delta.
    /// It seems that all other modules rely on this api
    pub async fn encode_async(
        mut self,
        rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<JoinHandle<()>, GitError> {
        Ok(tokio::spawn(async move {
            if self.window_size == 0 {
                self.parallel_encode(rx).await.unwrap()
            } else {
                self.encode(rx).await.unwrap()
            }
        }))
    }

    /// async version of encode_with_zstdelta, result data will be returned by JoinHandle.
    pub async fn encode_async_with_zstdelta(
        mut self,
        rx: mpsc::Receiver<MetaAttached<Entry, EntryMeta>>,
    ) -> Result<JoinHandle<()>, GitError> {
        Ok(tokio::spawn(async move {
            // Do not use parallel encode with zstdelta because it make no sense.
            self.encode_with_zstdelta(rx).await.unwrap()
        }))
    }

    /// Generate idx file after pack file has been generated
    pub async fn encode_idx_file(&mut self) -> Result<(), GitError> {
        if self.idx_sender.is_none() {
            return Err(GitError::PackEncodeError(String::from(
                "idx sender is none",
            )));
        }
        self.generate_idx_file().await?;
        // drop sender so downstream consumer can finish
        self.idx_sender.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{env, io::Cursor, path::PathBuf, sync::Arc, time::Instant};

    use tempfile::tempdir;
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        hash::{HashKind, ObjectHash, set_hash_kind_for_test},
        internal::{
            object::{blob::Blob, tree::TreeItem, types::ObjectType},
            pack::{Pack, tests::init_logger, utils::read_offset_encoding},
        },
        time_it,
    };

    /// Check if the given data is a valid pack file format by attempting to decode it.
    fn check_format(data: &Vec<u8>) {
        // Use a smaller cap on 32-bit targets to avoid usize overflow.
        let max_pack_size_u64 = if cfg!(target_pointer_width = "64") {
            6u64 * 1024 * 1024 * 1024
        } else {
            2u64 * 1024 * 1024 * 1024
        };
        let max_pack_size = usize::try_from(max_pack_size_u64).unwrap_or_else(|_| {
            panic!(
                "internal assertion failed: pack size cap {} does not fit in usize on this \
                 target; this should be unreachable given the target_pointer_width configuration",
                max_pack_size_u64
            )
        });
        let mut p = Pack::new(
            None,
            Some(max_pack_size), // 6GB on 64-bit, 2GB on 32-bit
            Some(PathBuf::from("/tmp/.cache_temp")),
            true,
        );
        let mut reader = Cursor::new(data);
        tracing::debug!("start check format");
        p.decode(&mut reader, |_| {}, None::<fn(ObjectHash)>)
            .expect("pack file format error");
    }

    fn test_entry(obj_type: ObjectType, data: Vec<u8>) -> Entry {
        Entry {
            obj_type,
            hash: ObjectHash::from_type_and_data(obj_type, &data),
            data,
            chain_len: 0,
        }
    }

    fn meta_entry(entry: Entry, file_path: Option<&str>) -> MetaAttached<Entry, EntryMeta> {
        MetaAttached {
            inner: entry,
            meta: EntryMeta {
                file_path: file_path.map(str::to_owned),
                ..EntryMeta::new()
            },
        }
    }

    fn tree_entry(tree: Tree) -> MetaAttached<Entry, EntryMeta> {
        meta_entry(tree.into(), None)
    }

    fn commit_entry_for_tree(tree_id: ObjectHash) -> MetaAttached<Entry, EntryMeta> {
        let data = format!("tree {tree_id}\n\nmessage\n").into_bytes();
        meta_entry(test_entry(ObjectType::Commit, data), None)
    }

    #[test]
    fn test_commit_root_tree_parses_first_line() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let tree_id = ObjectHash::from_str("0123456789abcdef0123456789abcdef01234567").unwrap();
        let commit = format!("tree {tree_id}\nparent deadbeef\n\nmessage\n");

        assert_eq!(commit_root_tree(commit.as_bytes()), Some(tree_id));
        assert_eq!(commit_root_tree(b"parent abc\n\nmessage\n"), None);
    }

    #[test]
    fn test_infer_blob_paths_recovers_nested_tree_paths() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let blob = Blob::from_content("hello");
        let nested_tree = Tree::from_tree_items(vec![TreeItem::new(
            TreeItemMode::Blob,
            blob.id,
            "file.rs".to_string(),
        )])
        .unwrap();
        let root_tree = Tree::from_tree_items(vec![TreeItem::new(
            TreeItemMode::Tree,
            nested_tree.id,
            "src".to_string(),
        )])
        .unwrap();
        let commit = commit_entry_for_tree(root_tree.id);
        let trees = vec![tree_entry(root_tree), tree_entry(nested_tree)];

        let paths = infer_blob_paths(&[commit], &trees);

        assert_eq!(paths.get(&blob.id).map(String::as_str), Some("src/file.rs"));
    }

    #[test]
    fn test_attach_inferred_blob_paths_preserves_existing_metadata() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let blob = Blob::from_content("hello");
        let tree = Tree::from_tree_items(vec![TreeItem::new(
            TreeItemMode::BlobExecutable,
            blob.id,
            "script.sh".to_string(),
        )])
        .unwrap();
        let commit = commit_entry_for_tree(tree.id);
        let trees = vec![tree_entry(tree)];
        let mut inferred_blobs = vec![meta_entry(blob.clone().into(), None)];
        let mut existing_blobs = vec![meta_entry(blob.into(), Some("keep/me"))];

        attach_inferred_blob_paths(std::slice::from_ref(&commit), &trees, &mut inferred_blobs);
        attach_inferred_blob_paths(&[commit], &trees, &mut existing_blobs);

        assert_eq!(
            inferred_blobs[0].meta.file_path.as_deref(),
            Some("script.sh")
        );
        assert_eq!(existing_blobs[0].meta.file_path.as_deref(), Some("keep/me"));
    }

    #[test]
    fn test_blob_delta_uses_same_path_zstdelta_when_smaller() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let base = Blob::from_content(&"abc123\n".repeat(512));
        let changed = Blob::from_content(&format!("{}tail\n", "abc123\n".repeat(512)));
        let blobs = vec![
            meta_entry(base.into(), Some("src/lib.rs")),
            meta_entry(changed.into(), Some("src/lib.rs")),
        ];

        let encoded = PackEncoder::try_blobs_as_offset_delta(blobs, 10, true).unwrap();

        assert_eq!(encoded.len(), 2);
        let encoded_delta = &encoded[1].0;
        let pack_type = (encoded_delta[0] >> 4) & 0x07;
        assert_eq!(
            ObjectType::from_pack_type_u8(pack_type).unwrap(),
            ObjectType::OffsetZstdelta
        );
    }

    #[test]
    fn test_blob_delta_uses_basename_anchor_outside_window() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        let base = Blob::from_content(&"shared-setting=true\n".repeat(512));
        let filler = Blob::from_content(&"different filler\n".repeat(512));
        let changed = Blob::from_content(&format!(
            "{}changed-setting=false\n",
            "shared-setting=true\n".repeat(512)
        ));
        let extra_same_name = Blob::from_content(&"third settings file\n".repeat(64));
        let blobs = vec![
            meta_entry(base.into(), Some("crate-a/settings.toml")),
            meta_entry(filler.into(), Some("crate-a/filler.txt")),
            meta_entry(changed.into(), Some("crate-b/settings.toml")),
            meta_entry(extra_same_name.into(), Some("crate-c/settings.toml")),
        ];

        let encoded = PackEncoder::try_blobs_as_offset_delta(blobs, 1, true).unwrap();

        assert_eq!(encoded.len(), 4);
        let encoded_delta = &encoded[2].0;
        let pack_type = (encoded_delta[0] >> 4) & 0x07;
        assert_eq!(
            ObjectType::from_pack_type_u8(pack_type).unwrap(),
            ObjectType::OffsetZstdelta
        );
    }

    #[tokio::test]
    async fn test_pack_encoder() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        async fn encode_once(window_size: usize) -> Vec<u8> {
            let (tx, mut rx) = mpsc::channel(100);
            let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

            // make some different objects, or decode will fail
            let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
            let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
            encoder.encode_async(entry_rx).await.unwrap();

            for str in str_vec {
                let blob = Blob::from_content(str);
                let entry: Entry = blob.into();
                entry_tx
                    .send(MetaAttached {
                        inner: entry,
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            // assert!(encoder.get_hash().is_some());
            let mut result = Vec::new();
            while let Some(chunk) = rx.recv().await {
                result.extend(chunk);
            }
            result
        }

        // without delta
        let pack_without_delta = encode_once(0).await;
        let pack_without_delta_size = pack_without_delta.len();
        check_format(&pack_without_delta);

        // with delta
        let pack_with_delta = encode_once(4).await;
        assert!(pack_with_delta.len() <= pack_without_delta_size);
        check_format(&pack_with_delta);
    }
    #[tokio::test]
    async fn test_pack_encoder_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);

        async fn encode_once(window_size: usize) -> Vec<u8> {
            let (tx, mut rx) = mpsc::channel(100);
            let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);

            let str_vec = vec!["hello, word", "hello, world.", "!", "123141251251"];
            let encoder = PackEncoder::new(str_vec.len(), window_size, tx);
            encoder.encode_async(entry_rx).await.unwrap();

            for s in str_vec {
                let blob = Blob::from_content(s);
                let entry: Entry = blob.into();
                entry_tx
                    .send(MetaAttached {
                        inner: entry,
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);

            let mut result = Vec::new();
            while let Some(chunk) = rx.recv().await {
                result.extend(chunk);
            }
            result
        }

        // without delta
        let pack_without_delta = encode_once(0).await;
        let pack_without_delta_size = pack_without_delta.len();
        check_format(&pack_without_delta);

        // with delta
        let pack_with_delta = encode_once(4).await;
        assert!(pack_with_delta.len() <= pack_without_delta_size);
        check_format(&pack_with_delta);
    }

    #[tokio::test]
    async fn test_pack_encoder_rejects_unencodable_ai_type_parallel() {
        let (tx, _rx) = mpsc::channel(8);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
        let mut encoder = PackEncoder::new(1, 0, tx);

        let mut entry: Entry = Blob::from_content("ai").into();
        entry.obj_type = ObjectType::Task;
        entry_tx
            .send(MetaAttached {
                inner: entry,
                meta: EntryMeta::new(),
            })
            .await
            .expect("send entry");
        drop(entry_tx);

        let err = encoder
            .encode(entry_rx)
            .await
            .expect_err("must reject AI pack type");
        assert!(matches!(err, GitError::PackEncodeError(_)));
    }

    #[tokio::test]
    async fn test_pack_encoder_rejects_unencodable_ai_type_delta_window() {
        let (tx, _rx) = mpsc::channel(8);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1);
        let mut encoder = PackEncoder::new(1, 10, tx);

        let mut entry: Entry = Blob::from_content("ai").into();
        entry.obj_type = ObjectType::Task;
        entry_tx
            .send(MetaAttached {
                inner: entry,
                meta: EntryMeta::new(),
            })
            .await
            .expect("send entry");
        drop(entry_tx);

        let err = encoder
            .encode(entry_rx)
            .await
            .expect_err("must reject AI pack type");
        assert!(matches!(err, GitError::PackEncodeError(_)));
    }

    async fn get_entries_for_test() -> Arc<Mutex<Vec<Entry>>> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/packs/encode-test-sha1.pack");

        let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

        let f = std::fs::File::open(&source).unwrap();
        tracing::info!("pack file size: {}", f.metadata().unwrap().len());
        let mut reader = std::io::BufReader::new(f);
        let entries = Arc::new(Mutex::new(Vec::new()));
        let entries_clone = entries.clone();
        p.decode(
            &mut reader,
            move |entry| {
                let mut entries = entries_clone.blocking_lock();
                entries.push(entry.inner);
            },
            None::<fn(ObjectHash)>,
        )
        .unwrap();
        assert_eq!(p.number, entries.lock().await.len());
        tracing::info!("total entries: {}", p.number);
        drop(p);

        entries
    }
    async fn get_entries_for_test_sha256() -> Arc<Mutex<Vec<Entry>>> {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/packs/encode-test-sha256.pack");

        let mut p = Pack::new(None, None, Some(PathBuf::from("/tmp/.cache_temp")), true);

        let f = std::fs::File::open(&source).unwrap();
        tracing::info!("pack file size: {}", f.metadata().unwrap().len());
        let mut reader = std::io::BufReader::new(f);
        let entries = Arc::new(Mutex::new(Vec::new()));
        let entries_clone = entries.clone();
        p.decode(
            &mut reader,
            move |entry| {
                let mut entries = entries_clone.blocking_lock();
                entries.push(entry.inner);
            },
            None::<fn(ObjectHash)>,
        )
        .unwrap();
        assert_eq!(p.number, entries.lock().await.len());
        tracing::info!("total entries: {}", p.number);
        drop(p);

        entries
    }

    #[tokio::test]
    async fn test_pack_encoder_parallel_large_file() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();

        let start = Instant::now();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        // encode entries with parallel
        let (tx, mut rx) = mpsc::channel(1_000_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test parallel encode", {
                encoder.parallel_encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", result.len());
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_parallel_large_file_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();

        let start = Instant::now();
        // use sha256 pack file for testing
        let entries = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(1_000_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(1_000_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test parallel encode sha256", {
                encoder.parallel_encode(entry_rx).await.unwrap();
            });
        });

        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("sha256 test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", result.len());
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        check_format(&result);
    }

    #[tokio::test]
    async fn test_pack_encoder_large_file() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        // encode entries
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test encode no parallel", {
                encoder.encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        // // only receive data
        // while (rx.recv().await).is_some() {
        //     // do nothing
        // }

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );
    }
    #[tokio::test]
    async fn test_pack_encoder_large_file_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let entries = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        // encode entries
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let mut encoder = PackEncoder::new(entries_number, 0, tx);
        tokio::spawn(async move {
            time_it!("test encode no parallel sha256", {
                encoder.encode(entry_rx).await.unwrap();
            });
        });

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        // // only receive data
        // while (rx.recv().await).is_some() {
        //     // do nothing
        // }

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );
    }

    #[tokio::test]
    async fn test_pack_encoder_with_zstdelta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);
        encoder.encode_async_with_zstdelta(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_with_zstdelta_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let entries = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();
        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);
        encoder.encode_async_with_zstdelta(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }

    #[test]
    fn test_encode_offset() {
        // let value = 11013;
        let value = 16389;

        let data = encode_offset(value);
        println!("{data:?}");
        let mut reader = Cursor::new(data);
        let (result, _) = read_offset_encoding(&mut reader).unwrap();
        println!("result: {result}");
        assert_eq!(result, value as u64);
    }

    #[tokio::test]
    async fn test_pack_encoder_large_file_with_delta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);

        let start = Instant::now(); // 开始时间
        encoder.encode_async(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }
    #[tokio::test]
    async fn test_pack_encoder_large_file_with_delta_sha256() {
        let _guard = set_hash_kind_for_test(HashKind::Sha256);
        init_logger();
        let entries = get_entries_for_test_sha256().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let (tx, mut rx) = mpsc::channel(100_000);
        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);

        let encoder = PackEncoder::new(entries_number, 10, tx);

        let start = Instant::now(); // 开始时间
        encoder.encode_async(entry_rx).await.unwrap();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        let mut result = Vec::new();
        while let Some(chunk) = rx.recv().await {
            result.extend(chunk);
        }

        let pack_size = result.len();
        let compression_rate = if total_original_size > 0 {
            1.0 - (pack_size as f64 / total_original_size as f64)
        } else {
            0.0
        };

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("new pack file size: {}", pack_size);
        tracing::info!("original total size: {}", total_original_size);
        tracing::info!("compression rate: {:.2}%", compression_rate * 100.0);
        tracing::info!(
            "space saved: {} bytes",
            total_original_size.saturating_sub(pack_size)
        );

        // check format
        check_format(&result);
    }

    #[tokio::test]
    async fn test_pack_encoder_output_to_files() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();

        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
        // 自动创建临时目录，生命周期结束自动删除
        let dir = tempdir().unwrap();
        let path = dir.path();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 0)
            .await
            .unwrap();

        // 验证临时目录下生成的 pack/idx 文件
        let mut pack_file = None;
        let mut idx_file = None;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            tracing::info!("file name: {:?}", file_name);
            let file_name = file_name.to_string_lossy();
            if file_name.ends_with(".pack") {
                pack_file = Some(entry.path());
            } else if file_name.ends_with(".idx") {
                idx_file = Some(entry.path());
            }
        }
        let pack_file = pack_file.expect("pack file not generated");
        let idx_file = idx_file.expect("idx file not generated");
        assert!(
            pack_file.metadata().unwrap().len() > 0,
            "pack file is empty"
        );
        assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("original total size: {}", total_original_size);
    }

    #[tokio::test]
    async fn test_pack_encoder_output_to_files_with_delta() {
        let _guard = set_hash_kind_for_test(HashKind::Sha1);
        init_logger();
        let entries = get_entries_for_test().await;
        let entries_number = entries.lock().await.len();

        let total_original_size: usize = entries
            .lock()
            .await
            .iter()
            .map(|entry| entry.data.len())
            .sum();

        let start = Instant::now();

        let (entry_tx, entry_rx) = mpsc::channel::<MetaAttached<Entry, EntryMeta>>(100_000);
        // 自动创建临时目录，生命周期结束自动删除
        let dir = tempdir().unwrap();
        let path = dir.path();

        // spawn a task to send entries
        tokio::spawn(async move {
            let entries = entries.lock().await;
            for entry in entries.iter() {
                entry_tx
                    .send(MetaAttached {
                        inner: entry.clone(),
                        meta: EntryMeta::new(),
                    })
                    .await
                    .unwrap();
            }
            drop(entry_tx);
            tracing::info!("all entries sent");
        });

        encode_and_output_to_files(entry_rx, entries_number, path.to_path_buf(), 10)
            .await
            .unwrap();

        // 验证临时目录下生成的 pack/idx 文件
        let mut pack_file = None;
        let mut idx_file = None;
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name();
            tracing::info!("file name: {:?}", file_name);
            let file_name = file_name.to_string_lossy();
            if file_name.ends_with(".pack") {
                pack_file = Some(entry.path());
            } else if file_name.ends_with(".idx") {
                idx_file = Some(entry.path());
            }
        }
        let pack_file = pack_file.expect("pack file not generated");
        let idx_file = idx_file.expect("idx file not generated");
        assert!(
            pack_file.metadata().unwrap().len() > 0,
            "pack file is empty"
        );
        assert!(idx_file.metadata().unwrap().len() > 0, "idx file is empty");

        let duration = start.elapsed();
        tracing::info!("test executed in: {:.2?}", duration);
        tracing::info!("original total size: {}", total_original_size);
    }
}
