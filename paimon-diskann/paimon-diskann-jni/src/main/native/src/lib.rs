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

//! JNI bindings for Apache Paimon's DiskANN vector index.
//!
//! This module uses Microsoft's official `diskann` Rust crate (v0.45.0)
//! from <https://github.com/microsoft/DiskANN> to provide graph-based
//! approximate nearest neighbor search via JNI.
//!
//! # JNI Safety
//!
//! Every `extern "system"` entry point is wrapped with [`std::panic::catch_unwind`]
//! so that a Rust panic never unwinds across the FFI boundary, which would cause
//! undefined behaviour and likely crash the JVM.  On panic the function throws a
//! `java.lang.RuntimeException` with the panic message and returns a safe default.

use jni::objects::{JByteBuffer, JClass, JObject, JPrimitiveArray, ReleaseMode};
use jni::sys::{jfloat, jint, jlong};
use jni::JNIEnv;

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};

use diskann::graph::test::provider as test_provider;
use diskann::graph::{self, DiskANNIndex};
use diskann::neighbor::{BackInserter, Neighbor};
use diskann_vector::distance::Metric;

mod paimon_fileio_provider;
mod pq;
use paimon_fileio_provider::FileIOProvider;

// ======================== Constants ========================

const METRIC_L2: i32 = 0;
const METRIC_INNER_PRODUCT: i32 = 1;
const METRIC_COSINE: i32 = 2;

/// The u32 ID reserved for the DiskANN graph start/entry point.
const START_POINT_ID: u32 = 0;

// ======================== Panic‐safe JNI helper ========================

/// Run `body` inside [`catch_unwind`].  If it panics, throw a Java
/// `RuntimeException` with the panic message and return `default`.
fn jni_catch_unwind<F, R>(env: &mut JNIEnv, default: R, body: F) -> R
where
    F: FnOnce() -> R + panic::UnwindSafe,
{
    match panic::catch_unwind(body) {
        Ok(v) => v,
        Err(payload) => {
            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown Rust panic in DiskANN JNI".to_string()
            };
            let _ = env.throw_new("java/lang/RuntimeException", msg);
            default
        }
    }
}

// ======================== Metric Mapping ========================

pub(crate) fn map_metric(metric_type: i32) -> Metric {
    match metric_type {
        METRIC_INNER_PRODUCT => Metric::InnerProduct,
        METRIC_COSINE => Metric::Cosine,
        _ => Metric::L2,
    }
}

// ======================== Index State ========================

struct IndexState {
    index: Arc<DiskANNIndex<test_provider::Provider>>,
    context: test_provider::Context,
    runtime: tokio::runtime::Runtime,

    dimension: i32,
    metric_type: i32,

    next_id: u32,

    /// Vectors stored in insertion order. Position i has int_id = i + 1.
    raw_data: Vec<Vec<f32>>,
}

// ======================== Registry ========================

struct IndexRegistry {
    next_handle: i64,
    indices: HashMap<i64, Arc<Mutex<IndexState>>>,
}

impl IndexRegistry {
    fn new() -> Self {
        Self {
            next_handle: 1,
            indices: HashMap::new(),
        }
    }

    fn insert(&mut self, state: IndexState) -> i64 {
        let handle = self.next_handle;
        self.next_handle += 1;
        self.indices.insert(handle, Arc::new(Mutex::new(state)));
        handle
    }
}

fn registry() -> &'static Mutex<IndexRegistry> {
    static REGISTRY: OnceLock<Mutex<IndexRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(IndexRegistry::new()))
}

fn get_index(handle: i64) -> Option<Arc<Mutex<IndexState>>> {
    let guard = registry().lock().ok()?;
    guard.indices.get(&handle).cloned()
}

// ======================== Index Construction ========================

fn create_index_state(
    dimension: i32,
    metric_type: i32,
    _index_type: i32,
    max_degree: i32,
    build_list_size: i32,
) -> Result<IndexState, String> {
    let dim = dimension as usize;
    let metric = map_metric(metric_type);
    let md = std::cmp::max(max_degree as usize, 4);
    let bls = std::cmp::max(build_list_size as usize, md);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to create tokio runtime: {}", e))?;

    let start_vector = vec![1.0f32; dim];
    let provider_config = test_provider::Config::new(
        metric,
        md,
        test_provider::StartPoint::new(START_POINT_ID, start_vector),
    )
    .map_err(|e| format!("Failed to create provider config: {:?}", e))?;
    let provider = test_provider::Provider::new(provider_config);

    let index_config = graph::config::Builder::new(
        md,
        graph::config::MaxDegree::same(),
        bls,
        metric.into(),
    )
    .build()
    .map_err(|e| format!("Failed to create index config: {:?}", e))?;

    let index = Arc::new(DiskANNIndex::new(index_config, provider, None));
    let context = test_provider::Context::default();

    Ok(IndexState {
        index,
        context,
        runtime,
        dimension,
        metric_type,
        next_id: START_POINT_ID + 1,
        raw_data: Vec::new(),
    })
}

// ======================== Buffer Helpers ========================

fn get_direct_buffer_slice<'a>(
    env: &mut JNIEnv,
    buffer: &JByteBuffer,
    len: usize,
) -> Option<&'a mut [u8]> {
    let ptr = env.get_direct_buffer_address(buffer).ok()?;
    let capacity = env.get_direct_buffer_capacity(buffer).ok()?;
    if capacity < len {
        return None;
    }
    unsafe { Some(std::slice::from_raw_parts_mut(ptr, len)) }
}

// ======================== Serialization Helpers ========================

fn write_i32(buf: &mut [u8], offset: &mut usize, v: i32) -> bool {
    if *offset + 4 > buf.len() { return false; }
    buf[*offset..*offset + 4].copy_from_slice(&v.to_ne_bytes());
    *offset += 4;
    true
}

fn write_f32(buf: &mut [u8], offset: &mut usize, v: f32) -> bool {
    if *offset + 4 > buf.len() { return false; }
    buf[*offset..*offset + 4].copy_from_slice(&v.to_ne_bytes());
    *offset += 4;
    true
}

// ======================== JNI Functions ========================

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    dimension: jint,
    metric_type: jint,
    index_type: jint,
    max_degree: jint,
    build_list_size: jint,
) -> jlong {
    let result = jni_catch_unwind(&mut env, 0i64, AssertUnwindSafe(|| -> jlong {
        match create_index_state(dimension, metric_type, index_type, max_degree, build_list_size) {
            Ok(state) => match registry().lock() {
                Ok(mut guard) => guard.insert(state),
                Err(_) => -1,
            },
            Err(_) => -2,
        }
    }));
    match result {
        -1 => { let _ = env.throw_new("java/lang/IllegalStateException", "DiskANN registry error"); 0 }
        -2 => { let _ = env.throw_new("java/lang/RuntimeException", "Failed to create DiskANN index"); 0 }
        v => v,
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexDestroy<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_catch_unwind(&mut env, (), AssertUnwindSafe(|| {
        if let Ok(mut guard) = registry().lock() {
            guard.indices.remove(&handle);
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexGetDimension<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_catch_unwind(&mut env, 0, AssertUnwindSafe(|| {
        get_index(handle)
            .and_then(|arc| arc.lock().ok().map(|s| s.dimension))
            .unwrap_or(0)
    }))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexGetCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_catch_unwind(&mut env, 0, AssertUnwindSafe(|| {
        get_index(handle)
            .and_then(|arc| arc.lock().ok().map(|s| s.raw_data.len() as jlong))
            .unwrap_or(0)
    }))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexGetMetricType<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_catch_unwind(&mut env, 0, AssertUnwindSafe(|| {
        get_index(handle)
            .and_then(|arc| arc.lock().ok().map(|s| s.metric_type))
            .unwrap_or(0)
    }))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexAdd<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    n: jlong,
    vector_buffer: JByteBuffer<'local>,
) {
    let arc = match get_index(handle) {
        Some(a) => a,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid index handle");
            return;
        }
    };
    let mut state = match arc.lock() {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Index lock poisoned");
            return;
        }
    };

    let num = n as usize;
    let dimension = state.dimension as usize;
    let vec_len = num * dimension * 4;

    let vec_bytes = match get_direct_buffer_slice(&mut env, &vector_buffer, vec_len) {
        Some(slice) => slice,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid vector buffer");
            return;
        }
    };

    let vectors =
        unsafe { std::slice::from_raw_parts(vec_bytes.as_ptr() as *const f32, num * dimension) };

    let strat = test_provider::Strategy::new();

    for i in 0..num {
        let base = i * dimension;
        let vector = vectors[base..base + dimension].to_vec();

        let int_id = state.next_id;
        state.next_id += 1;
        state.raw_data.push(vector.clone());

        // catch_unwind around the DiskANN graph insert which may panic.
        let idx_clone = Arc::clone(&state.index);
        let ctx = &state.context;
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            state.runtime.block_on(idx_clone.insert(strat, ctx, &int_id, vector.as_slice()))
        }));

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("DiskANN insert failed for int_id {}: {}", int_id, e),
                );
                return;
            }
            Err(_) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("DiskANN insert panicked for int_id {}", int_id),
                );
                return;
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexBuild<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _build_list_size: jint,
) {
    jni_catch_unwind(&mut env, (), AssertUnwindSafe(|| {
        if get_index(handle).is_none() {
            // Will be caught below.
            panic!("Invalid index handle");
        }
    }));
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSearch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    n: jlong,
    query_vectors: JPrimitiveArray<'local, jfloat>,
    k: jint,
    search_list_size: jint,
    distances: JPrimitiveArray<'local, jfloat>,
    labels: JPrimitiveArray<'local, jlong>,
) {
    let num = n as usize;
    let top_k = k as usize;

    let arc = match get_index(handle) {
        Some(a) => a,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid index handle");
            return;
        }
    };

    // Read query vectors into owned Vec.
    let query: Vec<f32> = {
        let query_elements =
            match unsafe { env.get_array_elements(&query_vectors, ReleaseMode::NoCopyBack) } {
                Ok(arr) => arr,
                Err(_) => {
                    let _ = env.throw_new(
                        "java/lang/IllegalArgumentException",
                        "Invalid query vectors",
                    );
                    return;
                }
            };
        query_elements.iter().copied().collect()
    };

    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Index lock poisoned");
            return;
        }
    };

    let dimension = state.dimension as usize;
    let total_results = num * top_k;
    let mut result_distances = vec![f32::MAX; total_results];
    let mut result_labels = vec![-1i64; total_results];

    if !state.raw_data.is_empty() {
        let strat = test_provider::Strategy::new();

        for qi in 0..num {
            let query_vec = &query[qi * dimension..(qi + 1) * dimension];

            let search_k = top_k + 1;
            let l_value = std::cmp::max(search_list_size as usize, search_k);

            let params = match graph::SearchParams::new(search_k, l_value, None) {
                Ok(p) => p,
                Err(e) => {
                    let _ = env.throw_new(
                        "java/lang/IllegalArgumentException",
                        format!("Invalid search params: {}", e),
                    );
                    return;
                }
            };

            let mut neighbors = vec![Neighbor::<u32>::default(); search_k];

            // catch_unwind around graph search.
            let idx_clone = Arc::clone(&state.index);
            let ctx = &state.context;
            let search_result = panic::catch_unwind(AssertUnwindSafe(|| {
                state.runtime.block_on(idx_clone.search(
                    &strat,
                    ctx,
                    query_vec,
                    &params,
                    &mut BackInserter::new(&mut neighbors),
                ))
            }));

            let stats = match search_result {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    let _ = env.throw_new(
                        "java/lang/RuntimeException",
                        format!("DiskANN search failed: {}", e),
                    );
                    return;
                }
                Err(_) => {
                    let _ = env.throw_new(
                        "java/lang/RuntimeException",
                        "DiskANN search panicked",
                    );
                    return;
                }
            };

            let result_count = stats.result_count as usize;
            let mut count = 0;
            for ri in 0..result_count {
                if count >= top_k {
                    break;
                }
                let neighbor = &neighbors[ri];
                if neighbor.id == START_POINT_ID {
                    continue;
                }
                let idx = qi * top_k + count;
                result_labels[idx] = (neighbor.id as i64) - 1;
                result_distances[idx] = neighbor.distance;
                count += 1;
            }
        }
    }

    drop(state);

    // Write distances back.
    {
        let mut dist_elements =
            match unsafe { env.get_array_elements(&distances, ReleaseMode::CopyBack) } {
                Ok(arr) => arr,
                Err(_) => {
                    let _ =
                        env.throw_new("java/lang/IllegalArgumentException", "Invalid distances");
                    return;
                }
            };
        for i in 0..std::cmp::min(dist_elements.len(), result_distances.len()) {
            dist_elements[i] = result_distances[i];
        }
    }

    // Write labels back.
    {
        let mut label_elements =
            match unsafe { env.get_array_elements(&labels, ReleaseMode::CopyBack) } {
                Ok(arr) => arr,
                Err(_) => {
                    let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid labels");
                    return;
                }
            };
        for i in 0..std::cmp::min(label_elements.len(), result_labels.len()) {
            label_elements[i] = result_labels[i];
        }
    }
}

// ============================================================================
// Serialization format (graph + data, no header)
// ============================================================================
//
// The index file (.index) contains ONLY the graph adjacency lists:
//   Graph   : for each node (start point + user vectors):
//               int_id       : i32
//               neighbor_cnt : i32
//               neighbors    : neighbor_cnt × i32
//
// The data file (.data) contains ONLY raw vectors stored sequentially:
//   Data    : for each user vector (in order 0, 1, 2, ...):
//               vector       : dim × f32
//
//   The sequential position IS the ID.
//   The start point is NOT stored in the data file.
//   position = int_id - 1 for user vectors (int_id > 0).
//
// All metadata (dimension, metric, max_degree, build_list_size, count,
// start_id) is stored in DiskAnnIndexMeta — not in the file.
//
// During search, both graph and vector data are read on demand from
// Paimon FileIO-backed storage (local, HDFS, S3, OSS, etc.) via JNI callbacks:
//   - Graph:   FileIOGraphReader.readNeighbors(int)
//   - Vectors: FileIOVectorReader.readVector(long)
// ============================================================================

// ---- Searcher registry (handles backed by FileIOProvider) ----

struct SearcherState {
    /// The DiskANN index (holds the FileIOProvider which has PQ data + graph/vector readers).
    index: Arc<DiskANNIndex<FileIOProvider>>,
    dimension: i32,
    /// Minimum external ID for this index. ext_id = min_ext_id + vec_idx (0-based).
    min_ext_id: i64,
    /// Graph start/entry point (medoid) internal ID.
    start_id: u32,
    /// I/O context for beam search: JVM, reader refs, DirectByteBuffer pointers.
    io_ctx: BeamSearchIOContext,
}

/// I/O context for beam search.  Provides JNI access to graph reader (for
/// neighbor lists) and vector reader (for full-precision vectors on disk).
struct BeamSearchIOContext {
    /// JVM handle for attaching threads.
    jvm: jni::JavaVM,
    /// GlobalRef to the Java vector reader (`FileIOVectorReader`).
    vector_reader_ref: jni::objects::GlobalRef,
    /// GlobalRef to the Java graph reader (`FileIOGraphReader`).
    graph_reader_ref: jni::objects::GlobalRef,
    /// Native address of the single-vector DirectByteBuffer.
    single_buf_ptr: *mut f32,
    /// Vector dimension.
    dim: usize,
    /// Distance metric type (0=L2, 1=IP, 2=Cosine).
    metric_type: i32,
}

// SAFETY: same justification as FileIOProvider — JavaVM and GlobalRef are
// Send+Sync, raw pointer access is serialized by single-threaded runtime.
unsafe impl Send for BeamSearchIOContext {}
unsafe impl Sync for BeamSearchIOContext {}

/// Compute exact distance between two vectors.
fn compute_exact_distance(a: &[f32], b: &[f32], metric_type: i32) -> f32 {
    match metric_type {
        METRIC_INNER_PRODUCT => {
            // Negative inner product (larger IP = more similar → smaller distance).
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            -dot
        }
        METRIC_COSINE => {
            // 1 − cos_sim
            let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            let denom = norm_a * norm_b;
            if denom < 1e-30 { 1.0 } else { 1.0 - dot / denom }
        }
        _ => {
            // Squared L2 distance.
            a.iter().zip(b).map(|(x, y)| { let d = x - y; d * d }).sum()
        }
    }
}

struct SearcherRegistry {
    next_handle: i64,
    searchers: HashMap<i64, Arc<Mutex<SearcherState>>>,
}

impl SearcherRegistry {
    fn new() -> Self {
        Self { next_handle: 100_000, searchers: HashMap::new() }
    }
    fn insert(&mut self, state: SearcherState) -> i64 {
        let h = self.next_handle;
        self.next_handle += 1;
        self.searchers.insert(h, Arc::new(Mutex::new(state)));
        h
    }
}

fn searcher_registry() -> &'static Mutex<SearcherRegistry> {
    static REG: OnceLock<Mutex<SearcherRegistry>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(SearcherRegistry::new()))
}

fn get_searcher(handle: i64) -> Option<Arc<Mutex<SearcherState>>> {
    searcher_registry().lock().ok()?.searchers.get(&handle).cloned()
}

// ======================== indexSerialize ========================

/// Serialize the index with its graph adjacency lists.
/// Returns the number of bytes written.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSerialize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    buffer: JByteBuffer<'local>,
) -> jlong {
    let arc = match get_index(handle) {
        Some(a) => a,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid handle"); return 0; }
    };
    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => { let _ = env.throw_new("java/lang/IllegalStateException", "Lock poisoned"); return 0; }
    };

    // Collect graph data from the underlying DiskANN test_provider.
    let provider = state.index.provider();

    let dim = state.dimension as usize;
    let num_user_vectors = state.raw_data.len();
    let num_nodes = num_user_vectors + 1; // +1 for start point

    // Build ordered list of (int_id, neighbors) using the async
    // NeighborAccessor API run synchronously on our tokio runtime.
    // Node order: start point (int_id=0) first, then user vectors (int_id=1,2,...).
    let mut graph_section_size: usize = 0;
    let mut graph_entries: Vec<(u32, Vec<u32>)> = Vec::with_capacity(num_nodes);

    for int_id in 0..num_nodes as u32 {
        let mut neighbors = Vec::new();
        {
            use diskann::graph::AdjacencyList;
            use diskann::provider::{DefaultAccessor, NeighborAccessor as NeighborAccessorTrait};

            let accessor = provider.default_accessor();
            let mut adj = AdjacencyList::<u32>::new();
            if state.runtime.block_on(accessor.get_neighbors(int_id, &mut adj)).is_ok() {
                neighbors = adj.iter().copied().collect();
            }
        }
        graph_section_size += 4 + 4 + neighbors.len() * 4; // int_id + cnt + neighbors
        graph_entries.push((int_id, neighbors));
    }

    // Data section: user vectors in sequential order (no start point).
    let data_section_size = num_user_vectors * dim * 4;
    let total_size = graph_section_size + data_section_size;

    let buf = match get_direct_buffer_slice(&mut env, &buffer, total_size) {
        Some(s) => s,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Buffer too small"); return 0; }
    };

    let mut off = 0usize;

    // Graph section: int_id(i32) + neighbor_cnt(i32) + neighbors(cnt × i32)
    for (int_id, neighbors) in &graph_entries {
        write_i32(buf, &mut off, *int_id as i32);
        write_i32(buf, &mut off, neighbors.len() as i32);
        for &n in neighbors {
            write_i32(buf, &mut off, n as i32);
        }
    }

    // Data section: user vectors in insertion order.
    for vec in &state.raw_data {
        for &v in vec {
            write_f32(buf, &mut off, v);
        }
    }

    total_size as jlong
}

/// Return the serialized size.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSerializeSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    let arc = match get_index(handle) {
        Some(a) => a,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid handle"); return 0; }
    };
    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => { let _ = env.throw_new("java/lang/IllegalStateException", "Lock poisoned"); return 0; }
    };

    let dim = state.dimension as usize;

    // Calculate size by iterating over all graph nodes (start point + user vectors).
    let provider = state.index.provider();
    let num_nodes = state.raw_data.len() + 1; // +1 for start point
    let mut graph_section_size: usize = 0;

    for int_id in 0..num_nodes as u32 {
        let neighbor_count = {
            use diskann::graph::AdjacencyList;
            use diskann::provider::{DefaultAccessor, NeighborAccessor as NeighborAccessorTrait};
            let accessor = provider.default_accessor();
            let mut adj = AdjacencyList::<u32>::new();
            if state.runtime.block_on(accessor.get_neighbors(int_id, &mut adj)).is_ok() {
                adj.len()
            } else {
                0
            }
        };
        graph_section_size += 4 + 4 + neighbor_count * 4; // int_id + cnt + neighbors
    }

    // Data section: only user vectors (no start point).
    let data_section_size = state.raw_data.len() * dim * 4;
    (graph_section_size + data_section_size) as jlong
}

// ======================== indexCreateSearcherFromReaders ========================

/// Create a search-only handle from two on-demand Java readers: one for graph
/// structure and one for vectors, plus PQ data for in-memory approximate
/// distance computation during beam search.
///
/// `graphReader`:    Java object with `readNeighbors(int)`, `getDimension()`, etc.
/// `vectorReader`:   Java object with `loadVector(long)`, DirectByteBuffer accessors.
/// `min_ext_id`:     Minimum external ID for int_id → ext_id conversion.
/// `pq_pivots`:      Serialized PQ codebook (byte[]).  Must not be null.
/// `pq_compressed`:  Serialized PQ compressed codes (byte[]).  Must not be null.
///
/// Returns a searcher handle (≥100000) for use with `indexSearchWithReader`.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexCreateSearcherFromReaders<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    graph_reader: JObject<'local>,
    vector_reader: JObject<'local>,
    min_ext_id: jlong,
    pq_pivots: JObject<'local>,
    pq_compressed: JObject<'local>,
) -> jlong {
    // Helper to call int-returning methods on graphReader.
    macro_rules! call_int {
        ($name:expr) => {
            match env.call_method(&graph_reader, $name, "()I", &[]) {
                Ok(v) => match v.i() { Ok(i) => i, Err(_) => { let _ = env.throw_new("java/lang/RuntimeException", concat!("Bad return from ", stringify!($name))); return 0; } },
                Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("Failed to call {}: {}", $name, e)); return 0; }
            }
        }
    }

    let dimension   = call_int!("getDimension");
    let metric_type = call_int!("getMetricValue");
    let max_degree  = call_int!("getMaxDegree") as usize;
    let build_ls    = call_int!("getBuildListSize") as usize;
    let count       = call_int!("getCount") as usize;
    let start_id    = call_int!("getStartId") as u32;
    let dim         = dimension as usize;

    // Create global refs for both readers.
    let global_graph_reader = match env.new_global_ref(&graph_reader) {
        Ok(g) => g,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("graph ref: {}", e)); return 0; }
    };
    let global_vector_reader = match env.new_global_ref(&vector_reader) {
        Ok(g) => g,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("vector ref: {}", e)); return 0; }
    };

    let jvm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("get JVM: {}", e)); return 0; }
    };

    // ---- Obtain DirectByteBuffer native pointers from the vector reader ----

    // Single-vector DirectByteBuffer: getDirectBuffer() → ByteBuffer
    let single_buf_ptr: *mut f32 = {
        let buf_obj = match env.call_method(&vector_reader, "getDirectBuffer", "()Ljava/nio/ByteBuffer;", &[]) {
            Ok(v) => match v.l() { Ok(o) => o, Err(_) => { let _ = env.throw_new("java/lang/RuntimeException", "Bad return from getDirectBuffer"); return 0; } },
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("getDirectBuffer: {}", e)); return 0; }
        };
        let byte_buf = jni::objects::JByteBuffer::from(buf_obj);
        match env.get_direct_buffer_address(&byte_buf) {
            Ok(ptr) => ptr as *mut f32,
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("GetDirectBufferAddress (single): {}", e)); return 0; }
        }
    };

    // Batch DirectByteBuffer: getBatchBuffer() → ByteBuffer
    let batch_buf_ptr: *mut f32 = {
        let buf_obj = match env.call_method(&vector_reader, "getBatchBuffer", "()Ljava/nio/ByteBuffer;", &[]) {
            Ok(v) => match v.l() { Ok(o) => o, Err(_) => { let _ = env.throw_new("java/lang/RuntimeException", "Bad return from getBatchBuffer"); return 0; } },
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("getBatchBuffer: {}", e)); return 0; }
        };
        let byte_buf = jni::objects::JByteBuffer::from(buf_obj);
        match env.get_direct_buffer_address(&byte_buf) {
            Ok(ptr) => ptr as *mut f32,
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("GetDirectBufferAddress (batch): {}", e)); return 0; }
        }
    };

    // Max batch size from the vector reader.
    let max_batch_size: usize = match env.call_method(&vector_reader, "getMaxBatchSize", "()I", &[]) {
        Ok(v) => match v.i() { Ok(i) => i as usize, Err(_) => max_degree },
        Err(_) => max_degree,
    };

    // ---- Deserialize PQ data (always required — Java has validated this) ----

    if pq_pivots.is_null() || pq_compressed.is_null() {
        let _ = env.throw_new("java/lang/IllegalArgumentException", "PQ pivots and compressed data must not be null");
        return 0;
    }

    let pivots_bytes: Vec<u8> = match env.convert_byte_array(
        jni::objects::JByteArray::from(pq_pivots),
    ) {
        Ok(b) if !b.is_empty() => b,
        _ => { let _ = env.throw_new("java/lang/IllegalArgumentException", "PQ pivots byte array is empty"); return 0; }
    };
    let compressed_bytes: Vec<u8> = match env.convert_byte_array(
        jni::objects::JByteArray::from(pq_compressed),
    ) {
        Ok(b) if !b.is_empty() => b,
        _ => { let _ = env.throw_new("java/lang/IllegalArgumentException", "PQ compressed byte array is empty"); return 0; }
    };

    let pq_state = match paimon_fileio_provider::PQState::deserialize(&pivots_bytes, &compressed_bytes) {
        Ok(pq) => pq,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("PQ deserialization failed: {}", e));
            return 0;
        }
    };

    // ---- Create beam search I/O context (separate GlobalRefs + JVM for search I/O) ----

    let io_vector_ref = match env.new_global_ref(&vector_reader) {
        Ok(g) => g,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("io vector ref: {}", e)); return 0; }
    };
    let io_graph_ref = match env.new_global_ref(&graph_reader) {
        Ok(g) => g,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("io graph ref: {}", e)); return 0; }
    };
    let io_jvm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("io JVM: {}", e)); return 0; }
    };
    let io_ctx = BeamSearchIOContext {
        jvm: io_jvm,
        vector_reader_ref: io_vector_ref,
        graph_reader_ref: io_graph_ref,
        single_buf_ptr,
        dim,
        metric_type,
    };

    // Start point is not stored in data file; use a dummy vector.
    let start_vec = vec![1.0f32; dim];

    // Build the FileIOProvider with on-demand graph reading and zero-copy vector access.
    let provider = FileIOProvider::new_with_readers(
        count,
        start_id,
        start_vec,
        jvm,
        global_vector_reader,
        global_graph_reader,
        dim,
        metric_type,
        max_degree,
        single_buf_ptr,
        batch_buf_ptr,
        max_batch_size,
        pq_state,
    );

    // Build DiskANNIndex config (still needed for the provider wrapper).
    let md = std::cmp::max(max_degree, 4);
    let bls = std::cmp::max(build_ls, md);
    let metric = map_metric(metric_type);

    let index_config = match graph::config::Builder::new(
        md,
        graph::config::MaxDegree::same(),
        bls,
        metric.into(),
    ).build() {
        Ok(c) => c,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("config: {:?}", e)); return 0; }
    };

    let index = Arc::new(DiskANNIndex::new(index_config, provider, None));

    let searcher = SearcherState {
        index,
        dimension,
        min_ext_id,
        start_id,
        io_ctx,
    };

    match searcher_registry().lock() {
        Ok(mut guard) => guard.insert(searcher),
        Err(_) => { let _ = env.throw_new("java/lang/IllegalStateException", "Registry error"); 0 }
    }
}

// ======================== indexSearchWithReader ========================

/// Search on a searcher handle created by `indexCreateSearcherFromReaders`.
///
/// Implements the standard DiskANN search algorithm:
///
/// 1. **Start** from the medoid (graph entry point).
/// 2. **Beam Search loop**:
///    - Pop the unvisited node with the smallest **PQ distance** from the beam.
///    - **Disk I/O**: read its full vector + neighbor list via JNI.
///    - **Compute Exact**: use the full vector to compute exact distance, update
///      the result heap.
///    - **Expand**: for each neighbor, compute **PQ distance** (in-memory).
///    - **Push**: add neighbors with good PQ distance to the beam (capped at L).
/// 3. **Return** the result heap (already exactly sorted).
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSearchWithReader<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    n: jlong,
    query_vectors: JPrimitiveArray<'local, jfloat>,
    k: jint,
    search_list_size: jint,
    distances: JPrimitiveArray<'local, jfloat>,
    labels: JPrimitiveArray<'local, jlong>,
) {
    let num = n as usize;
    let top_k = k as usize;

    let arc = match get_searcher(handle) {
        Some(a) => a,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid searcher handle");
            return;
        }
    };

    // Copy query vectors.
    let query: Vec<f32> = {
        let elems = match unsafe { env.get_array_elements(&query_vectors, ReleaseMode::NoCopyBack) } {
            Ok(a) => a,
            Err(_) => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid queries"); return; }
        };
        elems.iter().copied().collect()
    };

    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => { let _ = env.throw_new("java/lang/IllegalStateException", "Lock poisoned"); return; }
    };

    let dimension = state.dimension as usize;
    let total = num * top_k;
    let mut result_dist = vec![f32::MAX; total];
    let mut result_lbl  = vec![-1i64; total];

    let provider = state.index.provider();
    let pq = &provider.pq_state;
    let io = &state.io_ctx;
    let start_id = state.start_id;

    // Attach JNI thread once for all queries.
    let mut jni_env = match io.jvm.attach_current_thread() {
        Ok(e) => e,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("JVM attach: {}", e));
            return;
        }
    };

    for qi in 0..num {
        let qvec = &query[qi * dimension..(qi + 1) * dimension];
        let l = std::cmp::max(search_list_size as usize, top_k);

        // ---- Pre-compute PQ distance lookup table for this query ----
        let distance_table = pq.compute_distance_table(qvec, io.metric_type);

        // ---- Beam: sorted candidate list, capped at L entries ----
        // Each entry: (pq_distance, internal_node_id).
        // Sorted ascending by pq_distance so beam[0] is always the closest.
        let mut beam: Vec<(f32, u32)> = Vec::with_capacity(l + 1);
        let mut visited = std::collections::HashSet::<u32>::with_capacity(l * 2);

        // ---- Result heap: max-heap of (exact_distance, vec_idx) capped at top_k ----
        let mut results: Vec<(f32, usize)> = Vec::with_capacity(top_k + 1);
        let mut result_worst = f32::MAX;

        // Seed beam with the start point (medoid).
        // start_id is an internal node ID; vec_idx = start_id - 1.
        {
            let start_vec_idx = (start_id as usize).wrapping_sub(1);
            let start_pq_dist = if start_vec_idx < pq.num_vectors {
                pq.adc_distance(start_vec_idx, &distance_table, io.metric_type)
            } else {
                f32::MAX
            };
            beam.push((start_pq_dist, start_id));
        }

        // ---- Beam search loop ----
        loop {
            // Find the closest unvisited candidate in the beam.
            let next = beam.iter()
                .enumerate()
                .filter(|(_, (_, id))| !visited.contains(id))
                .min_by(|a, b| a.1.0.partial_cmp(&b.1.0).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(idx, &(dist, id))| (idx, dist, id));

            let (_beam_idx, _pq_dist, node_id) = match next {
                Some(t) => t,
                None => break, // No more unvisited candidates — convergence.
            };

            visited.insert(node_id);

            // ---- Disk I/O: read full vector for this node ----
            // (skip start point if it has no data vector — int_id 0 is synthetic)
            let vec_idx = (node_id as usize).wrapping_sub(1);

            if node_id != START_POINT_ID && vec_idx < pq.num_vectors {
                let position = vec_idx as i64;
                let load_ok = jni_env.call_method(
                    &io.vector_reader_ref,
                    "loadVector",
                    "(J)Z",
                    &[jni::objects::JValue::Long(position)],
                );
                if let Ok(v) = load_ok {
                    if let Ok(true) = v.z() {
                        let full_vec = unsafe {
                            std::slice::from_raw_parts(io.single_buf_ptr, io.dim)
                        };
                        let exact_dist = compute_exact_distance(qvec, full_vec, io.metric_type);

                        // Update result heap (keep top_k smallest exact distances).
                        if results.len() < top_k {
                            results.push((exact_dist, vec_idx));
                            if results.len() == top_k {
                                result_worst = results.iter()
                                    .map(|e| e.0)
                                    .fold(f32::NEG_INFINITY, f32::max);
                            }
                        } else if exact_dist < result_worst {
                            // Replace the worst entry.
                            if let Some(pos) = results.iter().position(|e| e.0 == result_worst) {
                                results[pos] = (exact_dist, vec_idx);
                                result_worst = results.iter()
                                    .map(|e| e.0)
                                    .fold(f32::NEG_INFINITY, f32::max);
                            }
                        }
                    }
                }
            }

            // ---- Read neighbor list for this node ----
            let neighbors: Vec<u32> = {
                // Try graph cache first.
                if let Some(term) = provider.graph.get(&node_id) {
                    term.neighbors.iter().copied().collect()
                } else {
                    // Fetch from graph reader via JNI.
                    let ret = jni_env.call_method(
                        &io.graph_reader_ref,
                        "readNeighbors",
                        "(I)[I",
                        &[jni::objects::JValue::Int(node_id as i32)],
                    );
                    match ret {
                        Ok(v) => match v.l() {
                            Ok(obj) if !obj.is_null() => {
                                let int_array = jni::objects::JIntArray::from(obj);
                                let arr_len = jni_env.get_array_length(&int_array).unwrap_or(0) as usize;
                                let mut buf = vec![0i32; arr_len];
                                let _ = jni_env.get_int_array_region(&int_array, 0, &mut buf);
                                let nbrs: Vec<u32> = buf.into_iter().map(|v| v as u32).collect();
                                // Cache for future queries.
                                let adj = diskann::graph::AdjacencyList::from_iter_untrusted(nbrs.iter().copied());
                                provider.graph.insert(node_id, paimon_fileio_provider::GraphTerm { neighbors: adj });
                                nbrs
                            }
                            _ => Vec::new(),
                        },
                        Err(_) => {
                            let _ = jni_env.exception_clear();
                            Vec::new()
                        }
                    }
                }
            };

            // ---- Expand: compute PQ distance for each neighbor, add to beam ----
            let beam_worst = if beam.len() >= l {
                beam.last().map(|e| e.0).unwrap_or(f32::MAX)
            } else {
                f32::MAX
            };

            for &nbr_id in &neighbors {
                if visited.contains(&nbr_id) {
                    continue;
                }
                // Already in beam? Skip duplicate.
                if beam.iter().any(|&(_, id)| id == nbr_id) {
                    continue;
                }

                let nbr_vec_idx = (nbr_id as usize).wrapping_sub(1);
                let nbr_pq_dist = if nbr_vec_idx < pq.num_vectors {
                    pq.adc_distance(nbr_vec_idx, &distance_table, io.metric_type)
                } else {
                    f32::MAX
                };

                if beam.len() < l || nbr_pq_dist < beam_worst {
                    // Insert in sorted order.
                    let insert_pos = beam.partition_point(|e| e.0 < nbr_pq_dist);
                    beam.insert(insert_pos, (nbr_pq_dist, nbr_id));
                    // Trim to L entries.
                    if beam.len() > l {
                        beam.truncate(l);
                    }
                }
            }
        }

        // ---- Collect top-K results (sorted by exact distance) ----
        results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        for (cnt, &(exact_dist, vec_idx)) in results.iter().enumerate() {
            if cnt >= top_k {
                break;
            }
            let idx = qi * top_k + cnt;
            result_lbl[idx] = state.min_ext_id + vec_idx as i64;
            result_dist[idx] = exact_dist;
        }
    }

    drop(jni_env);
    drop(state);

    // Write back distances.
    {
        let mut de = match unsafe { env.get_array_elements(&distances, ReleaseMode::CopyBack) } {
            Ok(a) => a,
            Err(_) => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Bad distances"); return; }
        };
        for i in 0..std::cmp::min(de.len(), result_dist.len()) { de[i] = result_dist[i]; }
    }
    // Write back labels.
    {
        let mut le = match unsafe { env.get_array_elements(&labels, ReleaseMode::CopyBack) } {
            Ok(a) => a,
            Err(_) => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Bad labels"); return; }
        };
        for i in 0..std::cmp::min(le.len(), result_lbl.len()) { le[i] = result_lbl[i]; }
    }
}

/// Destroy a searcher handle.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexDestroySearcher<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_catch_unwind(&mut env, (), AssertUnwindSafe(|| {
        if let Ok(mut guard) = searcher_registry().lock() {
            guard.searchers.remove(&handle);
        }
    }));
}

// ======================== PQ Train & Encode ========================

/// Train a PQ codebook on the vectors stored in the index and encode all vectors.
///
/// Uses `diskann-quantization`'s `LightPQTrainingParameters` for K-Means++ / Lloyd
/// training and `BasicTable` for encoding.
///
/// Returns a `byte[][]` where:
///   `[0]` = serialized pivots (codebook)
///   `[1]` = serialized compressed codes
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_pqTrainAndEncode<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    num_subspaces: jint,
    max_samples: jint,
    kmeans_iters: jint,
) -> JObject<'local> {
    // Obtain the index state outside of catch_unwind so we can throw typed exceptions.
    let arc = match get_index(handle) {
        Some(a) => a,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid index handle");
            return JObject::null();
        }
    };
    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Index lock poisoned");
            return JObject::null();
        }
    };

    let dim = state.dimension as usize;
    let m = num_subspaces as usize;
    let max_s = max_samples as usize;
    let iters = kmeans_iters as usize;

    // Perform PQ training and encoding inside catch_unwind to prevent panics crossing FFI.
    let pq_result = {
        let raw_data = &state.raw_data;
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            pq::train_and_encode(raw_data, dim, m, max_s, iters)
        }));
        // Drop the lock before JNI object creation.
        drop(state);
        match result {
            Ok(Ok(r)) => r,
            Ok(Err(msg)) => {
                let _ = env.throw_new("java/lang/RuntimeException", msg);
                return JObject::null();
            }
            Err(payload) => {
                let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust panic in PQ training".to_string()
                };
                let _ = env.throw_new("java/lang/RuntimeException", msg);
                return JObject::null();
            }
        }
    };

    // Build Java byte[][] result.
    let pivots_array = match env.byte_array_from_slice(&pq_result.pivots_bytes) {
        Ok(a) => a,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create pivots byte[]: {}", e),
            );
            return JObject::null();
        }
    };
    let compressed_array = match env.byte_array_from_slice(&pq_result.compressed_bytes) {
        Ok(a) => a,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create compressed byte[]: {}", e),
            );
            return JObject::null();
        }
    };

    let byte_array_class = match env.find_class("[B") {
        Ok(c) => c,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to find [B class: {}", e),
            );
            return JObject::null();
        }
    };

    let result = match env.new_object_array(2, &byte_array_class, &JObject::null()) {
        Ok(a) => a,
        Err(e) => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                format!("Failed to create byte[][]: {}", e),
            );
            return JObject::null();
        }
    };

    if let Err(e) = env.set_object_array_element(&result, 0, &pivots_array) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to set pivots: {}", e),
        );
        return JObject::null();
    }
    if let Err(e) = env.set_object_array_element(&result, 1, &compressed_array) {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            format!("Failed to set compressed: {}", e),
        );
        return JObject::null();
    }

    result.into()
}
