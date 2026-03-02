/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! A DiskANN [`DataProvider`] backed by Paimon FileIO (local, HDFS, S3, OSS, etc.).
//!
//! Both the navigational graph (adjacency lists) and full-precision vectors
//! are stored in Paimon FileIO-backed storage and read on demand via JNI
//! callbacks to Java reader objects:
//!
//!  - **Graph**: read through `FileIOGraphReader.readNeighbors(int)`, which
//!    reads from a `SeekableInputStream` over the `.index` file.
//!  - **Vectors**: read through `FileIOVectorReader.loadVector(long)` (zero-copy
//!    via DirectByteBuffer) or `readVectorsBatch(long[], int)` (batch prefetch).
//!
//! Performance optimizations:
//!
//!  - **Zero-copy vector reads**: `loadVector` writes into a pre-allocated
//!    DirectByteBuffer.  The Rust side reads floats directly from the native
//!    memory address — no `float[]` allocation, no JNI array copy.
//!  - **Batch prefetch**: When a node's neighbors are fetched, all neighbor
//!    vectors are batch-prefetched in a single JNI call, populating the
//!    provider-level `vector_cache`.  Subsequent `get_element` calls hit cache.
//!  - **Graph cache**: A `DashMap` lazily caches graph entries to reduce
//!    repeated FileIO reads.

use std::collections::HashMap;

use dashmap::DashMap;
use diskann::graph::glue;
use diskann::graph::AdjacencyList;
use diskann::provider;
use diskann::{ANNError, ANNResult};
use diskann_vector::distance::Metric;

use jni::objects::GlobalRef;
use jni::JavaVM;

use crate::map_metric;

// ======================== Error ========================

#[derive(Debug, Clone)]
pub enum FileIOProviderError {
    InvalidId(u32),
    JniCallFailed(String),
}

impl std::fmt::Display for FileIOProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidId(id) => write!(f, "invalid vector id {}", id),
            Self::JniCallFailed(msg) => write!(f, "FileIO read failed: {}", msg),
        }
    }
}

impl std::error::Error for FileIOProviderError {}

impl From<FileIOProviderError> for ANNError {
    #[track_caller]
    fn from(e: FileIOProviderError) -> ANNError {
        ANNError::opaque(e)
    }
}

diskann::always_escalate!(FileIOProviderError);

// ======================== PQ State ========================

/// In-memory Product Quantization state for approximate distance computation.
///
/// During beam search, PQ-reconstructed vectors replace full-precision disk I/O,
/// making the search almost entirely in-memory.  Only the final top-K candidates
/// are re-ranked with full-precision vectors from disk.
#[derive(Debug)]
pub struct PQState {
    /// Number of PQ subspaces (M).
    pub num_subspaces: usize,
    /// Number of centroids per subspace (K).
    pub num_centroids: usize,
    /// Sub-vector dimension (dimension / M).
    pub sub_dim: usize,
    /// Full vector dimension.
    pub dimension: usize,
    /// Centroid data, laid out as: pivots[m * K * sub_dim + k * sub_dim .. + sub_dim].
    pub pivots: Vec<f32>,
    /// Compressed codes: codes[vec_idx * M + m] = centroid index for vector vec_idx, subspace m.
    pub codes: Vec<u8>,
    /// Number of encoded vectors.
    pub num_vectors: usize,
}

impl PQState {
    /// Deserialize PQ pivots and compressed codes from the byte arrays written by `pq.rs`.
    ///
    /// Pivots format:  i32 dim | i32 M | i32 K | i32 sub_dim | f32[M*K*sub_dim]
    /// Codes format:   i32 N   | i32 M | byte[N*M]
    pub fn deserialize(pivots_bytes: &[u8], compressed_bytes: &[u8]) -> Result<Self, String> {
        if pivots_bytes.len() < 16 {
            return Err("PQ pivots too small".into());
        }
        if compressed_bytes.len() < 8 {
            return Err("PQ compressed too small".into());
        }

        let dimension = i32::from_ne_bytes(pivots_bytes[0..4].try_into().unwrap()) as usize;
        let num_subspaces = i32::from_ne_bytes(pivots_bytes[4..8].try_into().unwrap()) as usize;
        let num_centroids = i32::from_ne_bytes(pivots_bytes[8..12].try_into().unwrap()) as usize;
        let sub_dim = i32::from_ne_bytes(pivots_bytes[12..16].try_into().unwrap()) as usize;

        let expected_pivots_data = num_subspaces * num_centroids * sub_dim * 4;
        if pivots_bytes.len() < 16 + expected_pivots_data {
            return Err(format!(
                "PQ pivots data too small: need {}, have {}",
                16 + expected_pivots_data,
                pivots_bytes.len()
            ));
        }

        // Parse pivots as f32 (native endian).
        let pivots: Vec<f32> = pivots_bytes[16..16 + expected_pivots_data]
            .chunks_exact(4)
            .map(|c| f32::from_ne_bytes(c.try_into().unwrap()))
            .collect();

        // Parse compressed header.
        let num_vectors = i32::from_ne_bytes(compressed_bytes[0..4].try_into().unwrap()) as usize;
        let m_check = i32::from_ne_bytes(compressed_bytes[4..8].try_into().unwrap()) as usize;
        if m_check != num_subspaces {
            return Err(format!(
                "PQ subspace mismatch: pivots M={}, compressed M={}",
                num_subspaces, m_check
            ));
        }

        let expected_codes = num_vectors * num_subspaces;
        if compressed_bytes.len() < 8 + expected_codes {
            return Err(format!(
                "PQ codes too small: need {}, have {}",
                8 + expected_codes,
                compressed_bytes.len()
            ));
        }

        let codes = compressed_bytes[8..8 + expected_codes].to_vec();

        Ok(Self {
            num_subspaces,
            num_centroids,
            sub_dim,
            dimension,
            pivots,
            codes,
            num_vectors,
        })
    }

    /// Reconstruct an approximate vector for the given 0-based vector index
    /// by looking up PQ centroid sub-vectors.
    ///
    /// Cost: M table lookups + dimension float copies — entirely in L1/L2 cache.
    #[inline]
    pub fn reconstruct(&self, vec_idx: usize, out: &mut [f32]) {
        debug_assert!(vec_idx < self.num_vectors);
        debug_assert!(out.len() >= self.dimension);
        let code_base = vec_idx * self.num_subspaces;
        for m in 0..self.num_subspaces {
            let code = self.codes[code_base + m] as usize;
            let src_offset = m * self.num_centroids * self.sub_dim + code * self.sub_dim;
            let dst_offset = m * self.sub_dim;
            out[dst_offset..dst_offset + self.sub_dim]
                .copy_from_slice(&self.pivots[src_offset..src_offset + self.sub_dim]);
        }
    }

    // ---- ADC (Asymmetric Distance Computation) for brute-force PQ search ----

    /// Pre-compute a distance table from a query vector to all PQ centroids.
    ///
    /// Returns `dt[m * K + k]` where:
    ///   - L2:  squared L2 distance between query sub-vector m and centroid (m, k)
    ///   - IP/Cosine: dot product between query sub-vector m and centroid (m, k)
    ///
    /// Cost: O(M * K * sub_dim) — computed once per query, amortized over all vectors.
    pub fn compute_distance_table(&self, query: &[f32], metric_type: i32) -> Vec<f32> {
        let m = self.num_subspaces;
        let k = self.num_centroids;
        let sd = self.sub_dim;
        let mut table = vec![0.0f32; m * k];
        for mi in 0..m {
            let q_start = mi * sd;
            let q_sub = &query[q_start..q_start + sd];
            for ki in 0..k {
                let c_off = mi * k * sd + ki * sd;
                let centroid = &self.pivots[c_off..c_off + sd];
                let val = if metric_type == 1 || metric_type == 2 {
                    // IP or Cosine: dot product per subspace.
                    q_sub.iter().zip(centroid).map(|(a, b)| a * b).sum::<f32>()
                } else {
                    // L2: squared L2 per subspace.
                    q_sub
                        .iter()
                        .zip(centroid)
                        .map(|(a, b)| {
                            let d = a - b;
                            d * d
                        })
                        .sum::<f32>()
                };
                table[mi * k + ki] = val;
            }
        }
        table
    }

    /// Compute the approximate PQ distance for one vector using a pre-computed
    /// distance table.  Cost: O(M) — just M table lookups.
    #[inline]
    pub fn adc_distance(&self, vec_idx: usize, table: &[f32], metric_type: i32) -> f32 {
        let base = vec_idx * self.num_subspaces;
        let k = self.num_centroids;
        let mut raw = 0.0f32;
        for mi in 0..self.num_subspaces {
            let code = self.codes[base + mi] as usize;
            raw += table[mi * k + code];
        }
        // IP/Cosine: negate dot product so that larger similarity = smaller distance.
        if metric_type == 1 || metric_type == 2 {
            -raw
        } else {
            raw
        }
    }

    /// Brute-force PQ search: scan all vectors with ADC, return the top-K
    /// closest as `(vec_idx, pq_distance)` sorted by ascending distance.
    ///
    /// This replaces the graph-based beam search when PQ data is available.
    /// The entire search is in-memory — zero graph I/O, zero vector disk I/O.
    ///
    /// Cost: O(M * K * sub_dim) for the distance table +
    ///       O(N * M) for scanning all vectors.
    pub fn brute_force_search(
        &self,
        query: &[f32],
        top_k: usize,
        metric_type: i32,
    ) -> Vec<(usize, f32)> {
        let table = self.compute_distance_table(query, metric_type);
        let k = top_k.min(self.num_vectors);
        if k == 0 {
            return Vec::new();
        }

        // Max-heap to keep the top-k smallest distances.
        // We store (distance, vec_idx) and the heap evicts the largest distance.
        let mut heap: Vec<(f32, usize)> = Vec::with_capacity(k);
        let mut heap_max = f32::MAX;

        for i in 0..self.num_vectors {
            let dist = self.adc_distance(i, &table, metric_type);
            if heap.len() < k {
                heap.push((dist, i));
                if heap.len() == k {
                    // Find the current maximum after filling the heap.
                    heap_max = heap.iter().map(|e| e.0).fold(f32::NEG_INFINITY, f32::max);
                }
            } else if dist < heap_max {
                // Replace the worst element.
                if let Some(pos) = heap.iter().position(|e| e.0 == heap_max) {
                    heap[pos] = (dist, i);
                    heap_max = heap.iter().map(|e| e.0).fold(f32::NEG_INFINITY, f32::max);
                }
            }
        }

        // Sort by ascending distance.
        heap.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        heap.into_iter().map(|(d, i)| (i, d)).collect()
    }
}

// ======================== Graph Term ========================

/// One entry in the graph cache: its neighbor list.
pub struct GraphTerm {
    pub neighbors: AdjacencyList<u32>,
}

// ======================== FileIOProvider ========================

/// DiskANN data provider backed by Paimon FileIO.
///
/// Graph neighbors and vectors are read on demand from FileIO-backed storage
/// (local, HDFS, S3, OSS, etc.) via JNI callbacks to Java reader objects.
///
/// Three levels of vector access (in priority order):
///   1. **PQ reconstruction** (in-memory, ~O(dim) CPU, no I/O) — used during
///      beam search when PQ data is available.
///   2. **Provider-level `vector_cache`** (exact vectors cached from reranking
///      or disk I/O).
///   3. **DirectByteBuffer disk I/O** (single JNI call + zero-copy read).
///
/// Graph neighbors are cached in a `DashMap<u32, GraphTerm>` (lazy, write-once).
pub struct FileIOProvider {
    /// Graph cache: internal_id → { neighbors }.
    pub graph: DashMap<u32, GraphTerm>,
    /// Provider-level vector cache (exact vectors from reranking / disk reads).
    vector_cache: DashMap<u32, Box<[f32]>>,
    /// Total number of nodes (start point + user vectors).
    num_nodes: usize,
    /// Start-point IDs and their vectors (always kept in memory).
    start_points: HashMap<u32, Vec<f32>>,
    /// JVM handle for attaching threads.
    jvm: JavaVM,
    /// Global reference to the Java vector reader object (`FileIOVectorReader`).
    reader_ref: GlobalRef,
    /// Global reference to the Java graph reader object (`FileIOGraphReader`).
    graph_reader_ref: Option<GlobalRef>,
    /// Vector dimension.
    dim: usize,
    /// Distance metric.
    metric: Metric,
    /// Max degree.
    max_degree: usize,
    /// Native memory address of the single-vector DirectByteBuffer.
    single_buf_ptr: *mut f32,
    /// Native memory address of the batch DirectByteBuffer.
    batch_buf_ptr: *mut f32,
    /// Maximum number of vectors in one batch read.
    max_batch_size: usize,
    /// PQ state for in-memory approximate distance computation during beam search.
    /// PQ is always present — Java validates this before creating the native searcher.
    pub pq_state: PQState,
}

impl std::fmt::Debug for FileIOProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIOProvider")
            .field("dim", &self.dim)
            .field("metric", &self.metric)
            .field("max_degree", &self.max_degree)
            .field("graph_size", &self.graph.len())
            .field("vector_cache_size", &self.vector_cache.len())
            .field("pq_enabled", &true)
            .finish()
    }
}

// SAFETY: JavaVM is Send+Sync, GlobalRef is Send+Sync.
// Raw pointers are stable (backed by Java DirectByteBuffer kept alive by GlobalRef).
// All access is serialized by single-threaded tokio runtime.
unsafe impl Send for FileIOProvider {}
unsafe impl Sync for FileIOProvider {}

impl FileIOProvider {
    /// Build a search-only provider with on-demand graph reading, zero-copy
    /// vector access, and PQ for in-memory approximate search.
    ///
    /// `single_buf_ptr` and `batch_buf_ptr` are native addresses obtained via
    /// JNI `GetDirectBufferAddress` on the Java reader's DirectByteBuffers.
    /// PQ is always required — Java validates this before creating the native searcher.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_readers(
        num_nodes: usize,
        start_id: u32,
        start_vec: Vec<f32>,
        jvm: JavaVM,
        reader_ref: GlobalRef,
        graph_reader_ref: GlobalRef,
        dim: usize,
        metric_type: i32,
        max_degree: usize,
        single_buf_ptr: *mut f32,
        batch_buf_ptr: *mut f32,
        max_batch_size: usize,
        pq_state: PQState,
    ) -> Self {
        let graph = DashMap::new();
        let vector_cache = DashMap::new();

        let mut start_points = HashMap::new();
        start_points.insert(start_id, start_vec);

        Self {
            graph,
            vector_cache,
            num_nodes,
            start_points,
            jvm,
            reader_ref,
            graph_reader_ref: Some(graph_reader_ref),
            dim,
            metric: map_metric(metric_type),
            max_degree,
            single_buf_ptr,
            batch_buf_ptr,
            max_batch_size,
            pq_state,
        }
    }

    /// PQ brute-force search: scan all PQ codes, return top-K candidates.
    ///
    /// Returns `vec of (vec_idx, pq_distance)`.
    /// `vec_idx` is 0-based (data file position); `int_id = vec_idx + 1`.
    pub fn pq_brute_force_search(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Vec<(usize, f32)> {
        let metric_type = match self.metric {
            Metric::InnerProduct => 1i32,
            Metric::Cosine => 2i32,
            _ => 0i32,
        };
        self.pq_state.brute_force_search(query, top_k, metric_type)
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    // ---- Graph I/O ----

    /// Fetch neighbor list from FileIO-backed storage via JNI callback to
    /// `graphReader.readNeighbors(int)`.
    fn fetch_neighbors(&self, int_id: u32) -> Result<Option<Vec<u32>>, FileIOProviderError> {
        let graph_ref = match &self.graph_reader_ref {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

        let result = env.call_method(
            graph_ref,
            "readNeighbors",
            "(I)[I",
            &[jni::objects::JValue::Int(int_id as i32)],
        );

        let ret_val = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = env.exception_clear();
                return Err(FileIOProviderError::JniCallFailed(format!(
                    "readNeighbors({}) failed: {}",
                    int_id, e
                )));
            }
        };

        let obj = match ret_val.l() {
            Ok(o) => o,
            Err(_) => return Ok(Some(Vec::new())),
        };

        if obj.is_null() {
            return Ok(Some(Vec::new()));
        }

        let int_array = jni::objects::JIntArray::from(obj);
        let len = env
            .get_array_length(&int_array)
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("get_array_length: {}", e)))?
            as usize;

        let mut buf = vec![0i32; len];
        env.get_int_array_region(&int_array, 0, &mut buf)
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("get_int_array_region: {}", e)))?;

        Ok(Some(buf.into_iter().map(|v| v as u32).collect()))
    }

    // ---- Vector I/O (zero-copy via DirectByteBuffer) ----

    /// Fetch a single vector via `loadVector(long)` and read from DirectByteBuffer.
    ///
    /// The Java method writes the vector into the pre-allocated DirectByteBuffer.
    /// We then read floats directly from the native address — no `float[]`
    /// allocation and no JNI array copy.
    fn fetch_vector(&self, position: i64) -> Result<Option<Vec<f32>>, FileIOProviderError> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

        let result = env.call_method(
            &self.reader_ref,
            "loadVector",
            "(J)Z",
            &[jni::objects::JValue::Long(position)],
        );

        let success = match result {
            Ok(v) => match v.z() {
                Ok(b) => b,
                Err(_) => false,
            },
            Err(e) => {
                let _ = env.exception_clear();
                return Err(FileIOProviderError::JniCallFailed(format!(
                    "loadVector({}) failed: {}",
                    position, e
                )));
            }
        };

        if !success {
            return Ok(None);
        }

        // Read floats directly from the DirectByteBuffer native address.
        // SAFETY: single_buf_ptr is valid (backed by Java DirectByteBuffer kept alive
        // by GlobalRef), and access is serialized (single-threaded tokio runtime).
        let vec = unsafe {
            let slice = std::slice::from_raw_parts(self.single_buf_ptr, self.dim);
            slice.to_vec()
        };

        Ok(Some(vec))
    }

    /// Batch-prefetch vectors into the provider-level `vector_cache`.
    ///
    /// Calls `readVectorsBatch(long[], int)` once via JNI, then reads all vectors
    /// from the batch DirectByteBuffer native address.  Each vector is inserted
    /// into `vector_cache` keyed by its internal node ID.
    ///
    /// `ids` contains internal node IDs (not positions).  Position = id − 1.
    fn prefetch_vectors(&self, ids: &[u32]) -> Result<(), FileIOProviderError> {
        if ids.is_empty() || self.batch_buf_ptr.is_null() {
            return Ok(());
        }

        let count = std::cmp::min(ids.len(), self.max_batch_size);

        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

        // Build Java long[] of positions (position = int_id − 1).
        let positions: Vec<i64> = ids[..count].iter().map(|&id| (id as i64) - 1).collect();
        let java_positions = env
            .new_long_array(count as i32)
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("new_long_array: {}", e)))?;
        env.set_long_array_region(&java_positions, 0, &positions)
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("set_long_array_region: {}", e)))?;

        // Single JNI call: readVectorsBatch(long[], int) → int
        // SAFETY: JLongArray wraps a JObject; we reinterpret the raw pointer.
        let positions_obj = unsafe {
            jni::objects::JObject::from_raw(java_positions.as_raw())
        };
        let result = env.call_method(
            &self.reader_ref,
            "readVectorsBatch",
            "([JI)I",
            &[
                jni::objects::JValue::Object(&positions_obj),
                jni::objects::JValue::Int(count as i32),
            ],
        );
        // Prevent double-free: positions_obj shares the raw handle with java_positions.
        std::mem::forget(positions_obj);

        let read_count = match result {
            Ok(v) => match v.i() {
                Ok(n) => n as usize,
                Err(_) => 0,
            },
            Err(e) => {
                let _ = env.exception_clear();
                return Err(FileIOProviderError::JniCallFailed(format!(
                    "readVectorsBatch failed: {}",
                    e
                )));
            }
        };

        // Read vectors from batch DirectByteBuffer native address and populate cache.
        // SAFETY: batch_buf_ptr is valid, access is serialized.
        for i in 0..read_count {
            let int_id = ids[i];
            if self.vector_cache.contains_key(&int_id) {
                continue; // already cached
            }
            let offset = i * self.dim;
            let vec = unsafe {
                let slice = std::slice::from_raw_parts(self.batch_buf_ptr.add(offset), self.dim);
                slice.to_vec()
            };
            self.vector_cache.insert(int_id, vec.into_boxed_slice());
        }

        Ok(())
    }
}

// ======================== Context ========================

/// Lightweight execution context for the FileIO provider.
#[derive(Debug, Clone, Default)]
pub struct FileIOContext;

impl std::fmt::Display for FileIOContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "paimon fileio context")
    }
}

impl provider::ExecutionContext for FileIOContext {}

// ======================== DataProvider ========================

impl provider::DataProvider for FileIOProvider {
    type Context = FileIOContext;
    type InternalId = u32;
    type ExternalId = u32;
    type Error = FileIOProviderError;

    fn to_internal_id(
        &self,
        _context: &FileIOContext,
        gid: &u32,
    ) -> Result<u32, FileIOProviderError> {
        if (*gid as usize) < self.num_nodes {
            Ok(*gid)
        } else {
            Err(FileIOProviderError::InvalidId(*gid))
        }
    }

    fn to_external_id(
        &self,
        _context: &FileIOContext,
        id: u32,
    ) -> Result<u32, FileIOProviderError> {
        if (id as usize) < self.num_nodes {
            Ok(id)
        } else {
            Err(FileIOProviderError::InvalidId(id))
        }
    }
}

// ======================== SetElement (stub — search only) ========================

impl provider::SetElement<[f32]> for FileIOProvider {
    type SetError = ANNError;
    type Guard = provider::NoopGuard<u32>;

    async fn set_element(
        &self,
        _context: &FileIOContext,
        _id: &u32,
        _element: &[f32],
    ) -> Result<Self::Guard, Self::SetError> {
        Err(ANNError::opaque(FileIOProviderError::JniCallFailed(
            "set_element not supported on search-only FileIOProvider".to_string(),
        )))
    }
}

// ======================== NeighborAccessor ========================

#[derive(Debug, Clone, Copy)]
pub struct FileIONeighborAccessor<'a> {
    provider: &'a FileIOProvider,
}

impl provider::HasId for FileIONeighborAccessor<'_> {
    type Id = u32;
}

impl provider::NeighborAccessor for FileIONeighborAccessor<'_> {
    async fn get_neighbors(
        self,
        id: Self::Id,
        neighbors: &mut AdjacencyList<Self::Id>,
    ) -> ANNResult<Self> {
        // 1. Try cached graph.
        if let Some(term) = self.provider.graph.get(&id) {
            neighbors.overwrite_trusted(&term.neighbors);
            // Batch-prefetch neighbor vectors that aren't cached yet.
            self.prefetch_neighbor_vectors(&term.neighbors);
            return Ok(self);
        }

        // 2. On-demand: fetch from FileIO-backed storage via graph reader JNI callback.
        if self.provider.graph_reader_ref.is_some() {
            let fetched = self.provider.fetch_neighbors(id)?;
            if let Some(neighbor_ids) = fetched {
                let adj = AdjacencyList::from_iter_untrusted(neighbor_ids.iter().copied());
                neighbors.overwrite_trusted(&adj);
                // Cache in the DashMap for subsequent accesses.
                self.provider.graph.insert(id, GraphTerm { neighbors: adj.clone() });
                // Batch-prefetch neighbor vectors.
                self.prefetch_neighbor_vectors(&adj);
                return Ok(self);
            }
        }

        Err(ANNError::opaque(FileIOProviderError::InvalidId(id)))
    }
}

impl FileIONeighborAccessor<'_> {
    /// Batch-prefetch vectors for neighbors that aren't already in the vector cache.
    ///
    /// PQ is always enabled — beam search uses PQ reconstruction (in-memory),
    /// so disk prefetch is a no-op.  This method is kept for the graph::Strategy
    /// trait requirement.
    fn prefetch_neighbor_vectors(&self, _adj: &AdjacencyList<u32>) {
        // No-op: PQ is always enabled, beam search uses in-memory PQ codes.
    }
}

impl provider::NeighborAccessorMut for FileIONeighborAccessor<'_> {
    async fn set_neighbors(self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<Self> {
        match self.provider.graph.get_mut(&id) {
            Some(mut term) => {
                term.neighbors.clear();
                term.neighbors.extend_from_slice(neighbors);
                Ok(self)
            }
            None => Err(ANNError::opaque(FileIOProviderError::InvalidId(id))),
        }
    }

    async fn append_vector(self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<Self> {
        match self.provider.graph.get_mut(&id) {
            Some(mut term) => {
                term.neighbors.extend_from_slice(neighbors);
                Ok(self)
            }
            None => Err(ANNError::opaque(FileIOProviderError::InvalidId(id))),
        }
    }
}

// ======================== DefaultAccessor ========================

impl provider::DefaultAccessor for FileIOProvider {
    type Accessor<'a> = FileIONeighborAccessor<'a>;

    fn default_accessor(&self) -> Self::Accessor<'_> {
        FileIONeighborAccessor { provider: self }
    }
}

// ======================== Accessor ========================

/// Accessor that fetches vectors with three cache levels:
///
/// 1. **Start-point** (always in memory)
/// 2. **Provider-level `vector_cache`** (populated by batch prefetch)
/// 3. **Per-search LRU cache** (local to this accessor)
/// 4. **DirectByteBuffer I/O** (fallback: single JNI call + zero-copy read)
pub struct FileIOAccessor<'a> {
    provider: &'a FileIOProvider,
    buffer: Box<[f32]>,
    cache: VectorCache,
}

impl<'a> FileIOAccessor<'a> {
    pub fn new(provider: &'a FileIOProvider, cache_size: usize) -> Self {
        let buffer = vec![0.0f32; provider.dim()].into_boxed_slice();
        Self {
            provider,
            buffer,
            cache: VectorCache::new(cache_size),
        }
    }
}

impl provider::HasId for FileIOAccessor<'_> {
    type Id = u32;
}

impl provider::Accessor for FileIOAccessor<'_> {
    type Extended = Box<[f32]>;
    type Element<'a> = &'a [f32] where Self: 'a;
    type ElementRef<'a> = &'a [f32];
    type GetError = FileIOProviderError;

    async fn get_element(
        &mut self,
        id: u32,
    ) -> Result<Self::Element<'_>, Self::GetError> {
        // 1. Start-point vectors are always in memory.
        if let Some(vec) = self.provider.start_points.get(&id) {
            self.buffer.copy_from_slice(vec);
            return Ok(&*self.buffer);
        }

        // 2. Provider-level vector cache (exact vectors from reranking / disk).
        if let Some(cached) = self.provider.vector_cache.get(&id) {
            self.buffer.copy_from_slice(&cached);
            return Ok(&*self.buffer);
        }

        // 3. Per-search LRU cache (exact vectors).
        if let Some(cached) = self.cache.get(id) {
            self.buffer.copy_from_slice(cached);
            return Ok(&*self.buffer);
        }

        // 4. PQ reconstruction: approximate vector, entirely in-memory, no disk I/O.
        //    During beam search this is the primary hot path.
        {
            let pq = &self.provider.pq_state;
            let vec_idx = (id as usize).wrapping_sub(1);
            if vec_idx < pq.num_vectors {
                pq.reconstruct(vec_idx, &mut self.buffer);
                return Ok(&*self.buffer);
            }
        }

        // 5. Fallback: fetch via DirectByteBuffer I/O (single JNI call + zero-copy read).
        let position = (id as i64) - 1;
        let fetched = self.provider.fetch_vector(position)?;

        match fetched {
            Some(vec) if vec.len() == self.provider.dim() => {
                self.buffer.copy_from_slice(&vec);
                self.provider
                    .vector_cache
                    .insert(id, vec.clone().into_boxed_slice());
                self.cache.put(id, vec.into_boxed_slice());
                Ok(&*self.buffer)
            }
            Some(vec) => Err(FileIOProviderError::JniCallFailed(format!(
                "loadVector({}) returned {} floats, expected {}",
                position,
                vec.len(),
                self.provider.dim()
            ))),
            None => Err(FileIOProviderError::InvalidId(id)),
        }
    }
}

// ======================== DelegateNeighbor ========================

impl<'this> provider::DelegateNeighbor<'this> for FileIOAccessor<'_> {
    type Delegate = FileIONeighborAccessor<'this>;

    fn delegate_neighbor(&'this mut self) -> Self::Delegate {
        FileIONeighborAccessor {
            provider: self.provider,
        }
    }
}

// ======================== BuildQueryComputer ========================

impl provider::BuildQueryComputer<[f32]> for FileIOAccessor<'_> {
    type QueryComputerError = diskann::error::Infallible;
    type QueryComputer = <f32 as diskann::utils::VectorRepr>::QueryDistance;

    fn build_query_computer(
        &self,
        from: &[f32],
    ) -> Result<Self::QueryComputer, Self::QueryComputerError> {
        Ok(<f32 as diskann::utils::VectorRepr>::query_distance(
            from,
            self.provider.metric(),
        ))
    }
}

// ======================== BuildDistanceComputer ========================

impl provider::BuildDistanceComputer for FileIOAccessor<'_> {
    type DistanceComputerError = diskann::error::Infallible;
    type DistanceComputer = <f32 as diskann::utils::VectorRepr>::Distance;

    fn build_distance_computer(
        &self,
    ) -> Result<Self::DistanceComputer, Self::DistanceComputerError> {
        Ok(<f32 as diskann::utils::VectorRepr>::distance(
            self.provider.metric(),
            Some(self.provider.dim()),
        ))
    }
}

// ======================== SearchExt ========================

impl glue::SearchExt for FileIOAccessor<'_> {
    fn starting_points(
        &self,
    ) -> impl std::future::Future<Output = ANNResult<Vec<u32>>> + Send {
        futures_util::future::ok(self.provider.start_points.keys().copied().collect())
    }
}

// ======================== Blanket traits ========================

impl glue::ExpandBeam<[f32]> for FileIOAccessor<'_> {}
impl glue::FillSet for FileIOAccessor<'_> {}

// ======================== Strategy ========================

/// Search-only strategy for the Paimon FileIO provider.
#[derive(Debug, Default, Clone, Copy)]
pub struct FileIOStrategy;

impl FileIOStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl glue::SearchStrategy<FileIOProvider, [f32]> for FileIOStrategy {
    type QueryComputer = <f32 as diskann::utils::VectorRepr>::QueryDistance;
    type PostProcessor = glue::CopyIds;
    type SearchAccessorError = diskann::error::Infallible;
    type SearchAccessor<'a> = FileIOAccessor<'a>;

    fn search_accessor<'a>(
        &'a self,
        provider: &'a FileIOProvider,
        _context: &'a FileIOContext,
    ) -> Result<FileIOAccessor<'a>, diskann::error::Infallible> {
        // Per-search LRU as last-resort cache (most hits come from vector_cache).
        Ok(FileIOAccessor::new(provider, 1024))
    }

    fn post_processor(&self) -> Self::PostProcessor {
        Default::default()
    }
}

// For insert (graph construction) — delegates to prune/search accessors.
// We implement InsertStrategy and PruneStrategy as stubs since the FileIOProvider
// is search-only.  DiskANNIndex::new() requires the Provider to be Sized but
// does NOT call insert methods unless we invoke index.insert().
impl glue::PruneStrategy<FileIOProvider> for FileIOStrategy {
    type DistanceComputer = <f32 as diskann::utils::VectorRepr>::Distance;
    type PruneAccessor<'a> = FileIOAccessor<'a>;
    type PruneAccessorError = diskann::error::Infallible;

    fn prune_accessor<'a>(
        &'a self,
        provider: &'a FileIOProvider,
        _context: &'a FileIOContext,
    ) -> Result<Self::PruneAccessor<'a>, Self::PruneAccessorError> {
        Ok(FileIOAccessor::new(provider, 1024))
    }
}

impl glue::InsertStrategy<FileIOProvider, [f32]> for FileIOStrategy {
    type PruneStrategy = Self;

    fn prune_strategy(&self) -> Self::PruneStrategy {
        *self
    }

    fn insert_search_accessor<'a>(
        &'a self,
        provider: &'a FileIOProvider,
        _context: &'a FileIOContext,
    ) -> Result<Self::SearchAccessor<'a>, Self::SearchAccessorError> {
        Ok(FileIOAccessor::new(provider, 1024))
    }
}

impl<'a> glue::AsElement<&'a [f32]> for FileIOAccessor<'a> {
    type Error = diskann::error::Infallible;
    fn as_element(
        &mut self,
        vector: &'a [f32],
        _id: Self::Id,
    ) -> impl std::future::Future<Output = Result<Self::Element<'_>, Self::Error>> + Send {
        std::future::ready(Ok(vector))
    }
}

// ======================== VectorCache (per-search LRU) ========================

/// Tiny LRU cache for per-search vector access.  Most hits should come from the
/// provider-level `vector_cache`; this is a last-resort fallback.
struct VectorCache {
    map: HashMap<u32, Box<[f32]>>,
    order: Vec<u32>,
    capacity: usize,
}

impl VectorCache {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn get(&self, id: u32) -> Option<&[f32]> {
        self.map.get(&id).map(|v| &**v)
    }

    fn put(&mut self, id: u32, vec: Box<[f32]>) {
        if self.map.contains_key(&id) {
            return;
        }
        if self.order.len() >= self.capacity {
            if let Some(evicted) = self.order.first().copied() {
                self.order.remove(0);
                self.map.remove(&evicted);
            }
        }
        self.order.push(id);
        self.map.insert(id, vec);
    }
}
