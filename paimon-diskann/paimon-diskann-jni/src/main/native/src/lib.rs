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

use jni::objects::{JByteArray, JByteBuffer, JClass, JPrimitiveArray, ReleaseMode};
use jni::sys::{jfloat, jint, jlong};
use jni::JNIEnv;

use std::collections::HashMap;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, OnceLock};

use diskann::graph::test::provider as test_provider;
use diskann::graph::{self, DiskANNIndex};
use diskann::neighbor::{BackInserter, Neighbor};
use diskann_vector::distance::Metric;

// ======================== Constants ========================

const METRIC_L2: i32 = 0;
const METRIC_INNER_PRODUCT: i32 = 1;
const METRIC_COSINE: i32 = 2;

/// Serialization magic number ("PDAN").
const MAGIC: i32 = 0x5044414E;
/// Serialization format version (2 = real DiskANN).
const SERIALIZE_VERSION: i32 = 2;

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

fn map_metric(metric_type: i32) -> Metric {
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

fn read_f32(buf: &[u8], offset: &mut usize) -> Option<f32> {
    if *offset + 4 > buf.len() { return None; }
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[*offset..*offset + 4]);
    *offset += 4;
    Some(f32::from_ne_bytes(b))
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

fn serialization_size(dimension: i32, count: usize) -> usize {
    8 * 4 + count * (8 + (dimension as usize) * 4)
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

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSerialize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    buffer: JByteBuffer<'local>,
) -> jlong {
    let arc = match get_index(handle) {
        Some(a) => a,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid index handle");
            return 0;
        }
    };
    let state = match arc.lock() {
        Ok(s) => s,
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Index lock poisoned");
            return 0;
        }
    };

    let count = state.raw_data.len();
    let required = serialization_size(state.dimension, count);

    let buf = match get_direct_buffer_slice(&mut env, &buffer, required) {
        Some(slice) => slice,
        None => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Buffer too small");
            return 0;
        }
    };

    let mut offset = 0usize;
    if !write_i32(buf, &mut offset, MAGIC)
        || !write_i32(buf, &mut offset, SERIALIZE_VERSION)
        || !write_i32(buf, &mut offset, state.dimension)
        || !write_i32(buf, &mut offset, state.metric_type)
        || !write_i32(buf, &mut offset, state.index_type)
        || !write_i32(buf, &mut offset, state.max_degree as i32)
        || !write_i32(buf, &mut offset, state.build_list_size as i32)
        || !write_i32(buf, &mut offset, count as i32)
    {
        let _ = env.throw_new("java/lang/IllegalStateException", "Serialize header failed");
        return 0;
    }

    for (id, vector) in &state.raw_data {
        if !write_i64(buf, &mut offset, *id) {
            let _ = env.throw_new("java/lang/IllegalStateException", "Serialize failed");
            return 0;
        }
        for &v in vector {
            if !write_f32(buf, &mut offset, v) {
                let _ = env.throw_new("java/lang/IllegalStateException", "Serialize failed");
                return 0;
            }
        }
    }

    required as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexSerializeSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_catch_unwind(&mut env, 0, AssertUnwindSafe(|| {
        get_index(handle)
            .and_then(|arc| {
                arc.lock()
                    .ok()
                    .map(|s| serialization_size(s.dimension, s.raw_data.len()) as jlong)
            })
            .unwrap_or(0)
    }))
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_diskann_DiskAnnNative_indexDeserialize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    data: JByteArray<'local>,
    _length: jlong,
) -> jlong {
    let bytes = match env.convert_byte_array(&data) {
        Ok(data) => data,
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid data");
            return 0;
        }
    };

    let mut offset = 0usize;

    let magic = match read_i32(&bytes, &mut offset) {
        Some(v) => v,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Data too short"); return 0; }
    };
    if magic != MAGIC {
        let _ = env.throw_new("java/lang/IllegalArgumentException", "Invalid magic number");
        return 0;
    }

    let version = match read_i32(&bytes, &mut offset) {
        Some(v) => v,
        None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Data too short"); return 0; }
    };

    let dimension = read_i32(&bytes, &mut offset).unwrap_or(0);
    let metric_type = read_i32(&bytes, &mut offset).unwrap_or(METRIC_L2);
    let index_type = read_i32(&bytes, &mut offset).unwrap_or(0);

    let (max_degree, build_list_size, count) = if version >= 2 {
        let md = read_i32(&bytes, &mut offset).unwrap_or(64);
        let bls = read_i32(&bytes, &mut offset).unwrap_or(100);
        let cnt = read_i32(&bytes, &mut offset).unwrap_or(0) as usize;
        (md, bls, cnt)
    } else if version == 1 {
        let cnt = read_i32(&bytes, &mut offset).unwrap_or(0) as usize;
        (64, 100, cnt)
    } else {
        let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Unsupported version: {}", version));
        return 0;
    };

    let dim = dimension as usize;
    let mut entries: Vec<(i64, Vec<f32>)> = Vec::with_capacity(count);
    for _ in 0..count {
        let id = match read_i64(&bytes, &mut offset) {
            Some(v) => v,
            None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated data"); return 0; }
        };
        let mut vector = Vec::with_capacity(dim);
        for _ in 0..dim {
            let v = match read_f32(&bytes, &mut offset) {
                Some(val) => val,
                None => { let _ = env.throw_new("java/lang/IllegalArgumentException", "Truncated data"); return 0; }
            };
            vector.push(v);
        }
        entries.push((id, vector));
    }

    let mut state =
        match create_index_state(dimension, metric_type, index_type, max_degree, build_list_size) {
            Ok(s) => s,
            Err(msg) => {
                let _ = env.throw_new("java/lang/RuntimeException", format!("Failed to create index during deserialization: {}", msg));
                return 0;
            }
        };

    let strat = test_provider::Strategy::new();
    for (ext_id, vector) in &entries {
        let int_id = state.next_id;
        state.next_id += 1;
        state.ext_to_int.insert(*ext_id, int_id);
        state.int_to_ext.insert(int_id, *ext_id);
        state.raw_data.push((*ext_id, vector.clone()));

        let idx_clone = Arc::clone(&state.index);
        let ctx = &state.context;
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            state.runtime.block_on(idx_clone.insert(strat, ctx, &int_id, vector.as_slice()))
        }));

        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                let _ = env.throw_new("java/lang/RuntimeException", format!("Deserialization insert failed for id {}: {}", ext_id, e));
                return 0;
            }
            Err(_) => {
                let _ = env.throw_new("java/lang/RuntimeException", format!("Deserialization insert panicked for id {}", ext_id));
                return 0;
            }
        }
    }

    match registry().lock() {
        Ok(mut guard) => guard.insert(state),
        Err(_) => {
            let _ = env.throw_new("java/lang/IllegalStateException", "DiskANN registry error");
            0
        }
    }
}
