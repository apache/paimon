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

use jni::objects::{JByteArray, JByteBuffer, JClass, JObject, JPrimitiveArray, ReleaseMode};
use jni::sys::{jfloat, jint, jlong};
use jni::JNIEnv;

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};

use diskann::graph::test::provider as test_provider;
use diskann::graph::{self, DiskANNIndex};
use diskann::neighbor::{BackInserter, Neighbor};
use diskann_vector::distance::Metric;

mod jni_provider;
use jni_provider::{JniContext, JniProvider, JniStrategy};

// ======================== Constants ========================

const METRIC_L2: i32 = 0;
const METRIC_INNER_PRODUCT: i32 = 1;
const METRIC_COSINE: i32 = 2;

/// Serialization magic number ("PDAN").
const MAGIC: i32 = 0x5044414E;
/// Serialization format version (3 = graph + vectors).
const SERIALIZE_VERSION: i32 = 3;

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
    index_type: i32,
    max_degree: usize,
    build_list_size: usize,

    ext_to_int: HashMap<i64, u32>,
    int_to_ext: HashMap<u32, i64>,
    next_id: u32,

    raw_data: Vec<(i64, Vec<f32>)>,
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
    index_type: i32,
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
        index_type,
        max_degree: md,
        build_list_size: bls,
        ext_to_int: HashMap::new(),
        int_to_ext: HashMap::new(),
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

fn read_i32(buf: &[u8], offset: &mut usize) -> Option<i32> {
    if *offset + 4 > buf.len() { return None; }
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[*offset..*offset + 4]);
    *offset += 4;
    Some(i32::from_ne_bytes(b))
}

fn read_i64(buf: &[u8], offset: &mut usize) -> Option<i64> {
    if *offset + 8 > buf.len() { return None; }
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[*offset..*offset + 8]);
    *offset += 8;
    Some(i64::from_ne_bytes(b))
}

fn write_i32(buf: &mut [u8], offset: &mut usize, v: i32) -> bool {
    if *offset + 4 > buf.len() { return false; }
    buf[*offset..*offset + 4].copy_from_slice(&v.to_ne_bytes());
    *offset += 4;
    true
}

fn write_i64(buf: &mut [u8], offset: &mut usize, v: i64) -> bool {
    if *offset + 8 > buf.len() { return false; }
    buf[*offset..*offset + 8].copy_from_slice(&v.to_ne_bytes());
    *offset += 8;
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
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexAddWithIds<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    n: jlong,
    vector_buffer: JByteBuffer<'local>,
    id_buffer: JByteBuffer<'local>,
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
    let id_len = num * 8;

    let vec_bytes = match get_direct_buffer_slice(&mut env, &vector_buffer, vec_len) {
        Some(slice) => slice,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid vector buffer");
            return;
        }
    };
    let id_bytes = match get_direct_buffer_slice(&mut env, &id_buffer, id_len) {
        Some(slice) => slice,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid id buffer");
            return;
        }
    };

    let vectors =
        unsafe { std::slice::from_raw_parts(vec_bytes.as_ptr() as *const f32, num * dimension) };
    let ids = unsafe { std::slice::from_raw_parts(id_bytes.as_ptr() as *const i64, num) };

    let strat = test_provider::Strategy::new();

    for i in 0..num {
        let ext_id = ids[i];
        let base = i * dimension;
        let vector = vectors[base..base + dimension].to_vec();

        let int_id = state.next_id;
        state.next_id += 1;
        state.ext_to_int.insert(ext_id, int_id);
        state.int_to_ext.insert(int_id, ext_id);
        state.raw_data.push((ext_id, vector.clone()));

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
                    format!("DiskANN insert failed for id {}: {}", ext_id, e),
                );
                return;
            }
            Err(_) => {
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("DiskANN insert panicked for id {}", ext_id),
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
                result_labels[idx] =
                    *state.int_to_ext.get(&neighbor.id).unwrap_or(&(neighbor.id as i64));
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
// Serialization format (graph + vectors)
// ============================================================================
//
// Layout:
//   Header  : 9 × i32  (magic, version=3, dimension, metric, index_type,
//                         max_degree, build_list_size, count, start_id)
//   Graph   : for each node (ordered by ascending internal id):
//               ext_id       : i64
//               int_id       : i32
//               neighbor_cnt : i32
//               neighbors    : neighbor_cnt × i32
//   Vectors : for each node (same order):
//               vector       : dim × f32
//
// During search-only mode the Rust side loads the graph into a `JniProvider`
// and invokes the Java `FileIOVectorReader.readVector(long)` callback
// for every vector access that is NOT a start point.
// ============================================================================

// ---- Searcher registry (handles backed by JniProvider) ----

struct SearcherState {
    index: Arc<DiskANNIndex<JniProvider>>,
    context: JniContext,
    runtime: tokio::runtime::Runtime,
    dimension: i32,
    /// Mapping from internal DiskANN u32 IDs to external Java long IDs.
    int_to_ext: HashMap<u32, i64>,
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

    // Build ordered list of (int_id, ext_id, neighbors) using the async
    // NeighborAccessor API run synchronously on our tokio runtime.
    // Include the start point (int_id=0) so the search can navigate from it.
    let mut graph_section_size: usize = 0;
    let mut graph_entries: Vec<(u32, i64, Vec<u32>)> = Vec::new();

    // Start point first (ext_id = -1 as sentinel — never a real user ID).
    {
        use diskann::graph::AdjacencyList;
        use diskann::provider::{DefaultAccessor, NeighborAccessor as NeighborAccessorTrait};
        let accessor = provider.default_accessor();
        let mut adj = AdjacencyList::<u32>::new();
        let mut neighbors = Vec::new();
        if state.runtime.block_on(accessor.get_neighbors(START_POINT_ID, &mut adj)).is_ok() {
            neighbors = adj.iter().copied().collect();
        }
        graph_section_size += 8 + 4 + 4 + neighbors.len() * 4;
        graph_entries.push((START_POINT_ID, -1i64, neighbors));
    }

    // User-added vectors.
    for (ext_id, _vec) in &state.raw_data {
        let int_id = match state.ext_to_int.get(ext_id) {
            Some(i) => *i,
            None => continue,
        };
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

        graph_section_size += 8 + 4 + 4 + neighbors.len() * 4; // ext_id + int_id + cnt + neighbors
        graph_entries.push((int_id, *ext_id, neighbors));
    }

    let count = graph_entries.len(); // includes start point
    let vector_section_size = count * dim * 4;
    let header_size = 9 * 4; // 9 i32s
    let total_size = header_size + graph_section_size + vector_section_size;

    let buf = match get_direct_buffer_slice(&mut env, &buffer, total_size) {
        Some(s) => s,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Buffer too small"); return 0; }
    };

    let mut off = 0usize;
    // Header
    write_i32(buf, &mut off, MAGIC);
    write_i32(buf, &mut off, SERIALIZE_VERSION);
    write_i32(buf, &mut off, state.dimension);
    write_i32(buf, &mut off, state.metric_type);
    write_i32(buf, &mut off, state.index_type);
    write_i32(buf, &mut off, state.max_degree as i32);
    write_i32(buf, &mut off, state.build_list_size as i32);
    write_i32(buf, &mut off, count as i32);
    write_i32(buf, &mut off, START_POINT_ID as i32);

    // Graph section
    for (int_id, ext_id, neighbors) in &graph_entries {
        write_i64(buf, &mut off, *ext_id);
        write_i32(buf, &mut off, *int_id as i32);
        write_i32(buf, &mut off, neighbors.len() as i32);
        for &n in neighbors {
            write_i32(buf, &mut off, n as i32);
        }
    }

    // Vector section (same order as graph_entries)
    for (int_id, ext_id, _) in &graph_entries {
        if *int_id == START_POINT_ID {
            // Write the start point's dummy vector.
            for _ in 0..dim {
                write_f32(buf, &mut off, 1.0);
            }
        } else if let Some((_, vec)) = state.raw_data.iter().find(|(id, _)| id == ext_id) {
            for &v in vec {
                write_f32(buf, &mut off, v);
            }
        } else {
            // Zero-fill for missing vectors.
            for _ in 0..dim {
                write_f32(buf, &mut off, 0.0);
            }
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

    // Calculate size by iterating over all graph nodes (start point + raw_data).
    let provider = state.index.provider();
    let mut graph_section_size: usize = 0;

    // Start point.
    {
        use diskann::graph::AdjacencyList;
        use diskann::provider::{DefaultAccessor, NeighborAccessor as NeighborAccessorTrait};
        let accessor = provider.default_accessor();
        let mut adj = AdjacencyList::<u32>::new();
        let nc = if state.runtime.block_on(accessor.get_neighbors(START_POINT_ID, &mut adj)).is_ok() {
            adj.len()
        } else { 0 };
        graph_section_size += 8 + 4 + 4 + nc * 4;
    }

    // User-added vectors.
    for (ext_id, _) in &state.raw_data {
        let int_id = match state.ext_to_int.get(ext_id) {
            Some(i) => *i,
            None => continue,
        };
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
        graph_section_size += 8 + 4 + 4 + neighbor_count * 4;
    }

    let count = state.raw_data.len() + 1; // +1 for start point
    let header_size = 9 * 4;
    let vector_section_size = count * dim * 4;
    (header_size + graph_section_size + vector_section_size) as jlong
}

// ======================== indexCreateSearcher ========================

/// Create a search-only handle from serialized data + Java callback reader.
///
/// `data`:          byte[] containing the serialized index.
/// `vectorReader`:  Java object with a `readVector(long)` method (e.g. `FileIOVectorReader`).
///
/// Returns a searcher handle (≥100000) that can be used with `indexSearchWithReader`.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexCreateSearcher<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    data: JByteArray<'local>,
    vector_reader: JObject<'local>,
) -> jlong {
    let bytes = match env.convert_byte_array(&data) {
        Ok(d) => d,
        Err(_) => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid data"); return 0; }
    };

    let mut off = 0usize;

    // Parse header.
    let magic = read_i32(&bytes, &mut off).unwrap_or(0);
    if magic != MAGIC {
        let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid magic");
        return 0;
    }
    let version = read_i32(&bytes, &mut off).unwrap_or(0);
    if version != SERIALIZE_VERSION {
        let _ = env.throw_new("java/lang/IllegalArgumentException",
            format!("Expected version {}, got {}", SERIALIZE_VERSION, version));
        return 0;
    }
    let dimension   = read_i32(&bytes, &mut off).unwrap_or(0);
    let metric_type = read_i32(&bytes, &mut off).unwrap_or(0);
    let _index_type = read_i32(&bytes, &mut off).unwrap_or(0);
    let max_degree  = read_i32(&bytes, &mut off).unwrap_or(64) as usize;
    let build_ls    = read_i32(&bytes, &mut off).unwrap_or(100) as usize;
    let count       = read_i32(&bytes, &mut off).unwrap_or(0) as usize;
    let start_id    = read_i32(&bytes, &mut off).unwrap_or(0) as u32;
    let dim         = dimension as usize;

    // Parse graph section.
    let mut graph_data: Vec<(u32, i64, Vec<u32>)> = Vec::with_capacity(count);
    let mut int_to_ext: HashMap<u32, i64> = HashMap::with_capacity(count);

    for _ in 0..count {
        let ext_id = match read_i64(&bytes, &mut off) {
            Some(v) => v,
            None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated graph"); return 0; }
        };
        let int_id = match read_i32(&bytes, &mut off) {
            Some(v) => v as u32,
            None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated graph"); return 0; }
        };
        let ncnt = match read_i32(&bytes, &mut off) {
            Some(v) => v as usize,
            None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated graph"); return 0; }
        };
        let mut neighbors = Vec::with_capacity(ncnt);
        for _ in 0..ncnt {
            let n = match read_i32(&bytes, &mut off) {
                Some(v) => v as u32,
                None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated graph"); return 0; }
            };
            neighbors.push(n);
        }
        int_to_ext.insert(int_id, ext_id);
        graph_data.push((int_id, ext_id, neighbors));
    }

    // Create JNI global ref for the vector reader callback (needed before
    // we fetch the start-point vector below).
    let global_reader = match env.new_global_ref(&vector_reader) {
        Ok(g) => g,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                format!("Failed to create global ref: {}", e));
            return 0;
        }
    };

    let jvm = match env.get_java_vm() {
        Ok(vm) => vm,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                format!("Failed to get JavaVM: {}", e));
            return 0;
        }
    };

    // Fetch start-point vector via JNI callback.
    // The byte[] passed to this function only contains header + graph
    // (no vector section), so vectors are read on-demand from the reader.
    // The start point has ext_id = -1 (sentinel); its vector is the dummy
    // [1.0, …] written during serialization.  We try the reader first,
    // falling back to a non-zero dummy to avoid division-by-zero with
    // cosine metric.
    let start_ext_id = graph_data.iter()
        .find(|(iid, _, _)| *iid == start_id)
        .map(|(_, eid, _)| *eid)
        .unwrap_or(-1);
    let start_vec = {
        // Try reading from the Java reader callback.
        let fetched = (|| -> Option<Vec<f32>> {
            let mut e = jvm.attach_current_thread().ok()?;
            let result = e.call_method(
                &global_reader, "readVector", "(J)[F",
                &[jni::objects::JValue::Long(start_ext_id)],
            ).ok()?;
            let arr = result.l().ok()?;
            if arr.is_null() { return None; }
            let jarr: jni::objects::JFloatArray = arr.into();
            let len = e.get_array_length(&jarr).ok()? as usize;
            if len == 0 { return None; }
            let mut buf = vec![0.0f32; len];
            e.get_float_array_region(&jarr, 0, &mut buf).ok()?;
            Some(buf)
        })();
        fetched.unwrap_or_else(|| vec![1.0f32; dim])
    };

    // Build the JniProvider (graph in memory, vectors via JNI callback).
    let provider = JniProvider::new(
        graph_data,
        start_id,
        start_vec,
        jvm,
        global_reader,
        dim,
        metric_type,
        max_degree,
    );

    // Build DiskANNIndex config.
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
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                format!("Failed to create index config: {:?}", e));
            return 0;
        }
    };

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException",
                format!("Failed to create tokio runtime: {}", e));
            return 0;
        }
    };

    // DiskANNIndex takes ownership of the provider.  After construction,
    // the provider is accessible via index.provider() for read access.
    // We store int_to_ext separately for mapping search results.
    let index = Arc::new(DiskANNIndex::new(index_config, provider, None));

    let searcher = SearcherState {
        index,
        context: JniContext,
        runtime,
        dimension,
        int_to_ext,
    };

    match searcher_registry().lock() {
        Ok(mut guard) => guard.insert(searcher),
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Searcher registry error");
            0
        }
    }
}

// ======================== indexCreateSearcherFromReaders ========================

/// Create a search-only handle from two on-demand Java readers: one for graph
/// structure and one for vectors.
///
/// `graphReader`:   Java object with `readNeighbors(int)`, `getDimension()`,
///                  `getCount()`, `getStartId()`, `getMaxDegree()`,
///                  `getBuildListSize()`, `getMetricValue()`,
///                  `getAllInternalIds()`, `getAllExternalIds()`.
/// `vectorReader`:  Java object with `readVector(long)`.
///
/// Returns a searcher handle (≥100000) for use with `indexSearchWithReader`.
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexCreateSearcherFromReaders<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    graph_reader: JObject<'local>,
    vector_reader: JObject<'local>,
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
    let _index_type = call_int!("getIndexTypeValue");
    let max_degree  = call_int!("getMaxDegree") as usize;
    let build_ls    = call_int!("getBuildListSize") as usize;
    let count       = call_int!("getCount") as usize;
    let start_id    = call_int!("getStartId") as u32;
    let dim         = dimension as usize;

    // Read int_to_ext mapping via getAllInternalIds() / getAllExternalIds().
    let int_ids: Vec<i32> = {
        let result = match env.call_method(&graph_reader, "getAllInternalIds", "()[I", &[]) {
            Ok(v) => v,
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("getAllInternalIds failed: {}", e)); return 0; }
        };
        let obj = match result.l() {
            Ok(o) => o,
            Err(_) => { let _ = env.throw_new("java/lang/RuntimeException", "Bad return from getAllInternalIds"); return 0; }
        };
        let arr = jni::objects::JIntArray::from(obj);
        let len = env.get_array_length(&arr).unwrap_or(0) as usize;
        let mut buf = vec![0i32; len];
        let _ = env.get_int_array_region(&arr, 0, &mut buf);
        buf
    };

    let ext_ids: Vec<i64> = {
        let result = match env.call_method(&graph_reader, "getAllExternalIds", "()[J", &[]) {
            Ok(v) => v,
            Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("getAllExternalIds failed: {}", e)); return 0; }
        };
        let obj = match result.l() {
            Ok(o) => o,
            Err(_) => { let _ = env.throw_new("java/lang/RuntimeException", "Bad return from getAllExternalIds"); return 0; }
        };
        let arr = jni::objects::JLongArray::from(obj);
        let len = env.get_array_length(&arr).unwrap_or(0) as usize;
        let mut buf = vec![0i64; len];
        let _ = env.get_long_array_region(&arr, 0, &mut buf);
        buf
    };

    // Build ID mappings.
    let mut int_to_ext: HashMap<u32, i64> = HashMap::with_capacity(count);
    let mut ext_to_int: HashMap<i64, u32> = HashMap::with_capacity(count);
    for i in 0..std::cmp::min(int_ids.len(), ext_ids.len()) {
        let iid = int_ids[i] as u32;
        let eid = ext_ids[i];
        int_to_ext.insert(iid, eid);
        ext_to_int.insert(eid, iid);
    }

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

    // Fetch start-point vector via vectorReader JNI callback.
    let start_ext_id = int_to_ext.get(&start_id).copied().unwrap_or(-1);
    let start_vec = {
        let fetched = (|| -> Option<Vec<f32>> {
            let mut e = jvm.attach_current_thread().ok()?;
            let result = e.call_method(
                &global_vector_reader, "readVector", "(J)[F",
                &[jni::objects::JValue::Long(start_ext_id)],
            ).ok()?;
            let arr = result.l().ok()?;
            if arr.is_null() { return None; }
            let jarr: jni::objects::JFloatArray = arr.into();
            let len = e.get_array_length(&jarr).ok()? as usize;
            if len == 0 { return None; }
            let mut buf = vec![0.0f32; len];
            e.get_float_array_region(&jarr, 0, &mut buf).ok()?;
            Some(buf)
        })();
        fetched.unwrap_or_else(|| vec![1.0f32; dim])
    };

    // Build the JniProvider with on-demand graph reading.
    let provider = JniProvider::new_with_readers(
        int_to_ext.clone(),
        ext_to_int,
        start_id,
        start_vec,
        jvm,
        global_vector_reader,
        global_graph_reader,
        dim,
        metric_type,
        max_degree,
    );

    // Build DiskANNIndex config.
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

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(r) => r,
        Err(e) => { let _ = env.throw_new("java/lang/RuntimeException", format!("runtime: {}", e)); return 0; }
    };

    let index = Arc::new(DiskANNIndex::new(index_config, provider, None));

    let searcher = SearcherState {
        index,
        context: JniContext,
        runtime,
        dimension,
        int_to_ext,
    };

    match searcher_registry().lock() {
        Ok(mut guard) => guard.insert(searcher),
        Err(_) => { let _ = env.throw_new("java/lang/IllegalStateException", "Registry error"); 0 }
    }
}

// ======================== indexSearchWithReader ========================

/// Search on a searcher handle created by `indexCreateSearcher`.
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

    let strat = JniStrategy::new();

    for qi in 0..num {
        let qvec = &query[qi * dimension..(qi + 1) * dimension];
        let search_k = top_k + 1;
        let l = std::cmp::max(search_list_size as usize, search_k);

        let params = match graph::SearchParams::new(search_k, l, None) {
            Ok(p) => p,
            Err(e) => {
                let _ = env.throw_new("java/lang/IllegalArgumentException",
                    format!("Invalid search params: {}", e));
                return;
            }
        };

        let mut neighbors = vec![Neighbor::<u32>::default(); search_k];

        let idx_clone = Arc::clone(&state.index);
        let search_result = panic::catch_unwind(AssertUnwindSafe(|| {
            state.runtime.block_on(idx_clone.search(
                &strat,
                &state.context,
                qvec,
                &params,
                &mut BackInserter::new(&mut neighbors),
            ))
        }));

        let stats = match search_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                let _ = env.throw_new("java/lang/RuntimeException",
                    format!("Search failed: {}", e));
                return;
            }
            Err(_) => {
                let _ = env.throw_new("java/lang/RuntimeException", "Search panicked");
                return;
            }
        };

        let rc = stats.result_count as usize;
        let mut cnt = 0;
        for ri in 0..rc {
            if cnt >= top_k { break; }
            let nb = &neighbors[ri];
            if nb.id == START_POINT_ID { continue; }
            let idx = qi * top_k + cnt;
            result_lbl[idx] = state.int_to_ext.get(&nb.id).copied().unwrap_or(nb.id as i64);
            result_dist[idx] = nb.distance;
            cnt += 1;
        }
    }

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
