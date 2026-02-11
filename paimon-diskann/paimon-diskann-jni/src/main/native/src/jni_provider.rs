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

//! A DiskANN [`DataProvider`] whose graph lives in memory but whose vectors
//! are fetched on demand from Java via JNI callbacks.
//!
//! This enables the classic DiskANN architecture:
//!  - **In-memory**: navigational graph (adjacency lists) + start-point vectors.
//!  - **On-demand**: full-precision vectors are read through
//!    `FileIOVectorReader.readVector(long)` on the Java side, which can
//!    be backed by Paimon FileIO (local, HDFS, S3, OSS, etc.).

use std::collections::HashMap;

use dashmap::DashMap;
use diskann::graph::glue;
use diskann::graph::AdjacencyList;
use diskann::provider;
use diskann::{ANNError, ANNResult};
use diskann_vector::distance::Metric;

use jni::objects::GlobalRef;
use jni::sys::jlong;
use jni::JavaVM;

use crate::map_metric;

// ======================== Error ========================

#[derive(Debug, Clone)]
pub enum JniProviderError {
    InvalidId(u32),
    JniCallFailed(String),
}

impl std::fmt::Display for JniProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidId(id) => write!(f, "invalid vector id {}", id),
            Self::JniCallFailed(msg) => write!(f, "JNI callback failed: {}", msg),
        }
    }
}

impl std::error::Error for JniProviderError {}

impl From<JniProviderError> for ANNError {
    #[track_caller]
    fn from(e: JniProviderError) -> ANNError {
        ANNError::opaque(e)
    }
}

diskann::always_escalate!(JniProviderError);

// ======================== LRU Cache ========================

/// Tiny LRU cache for recently fetched vectors to reduce JNI round-trips.
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

// ======================== Graph Term ========================

/// One entry in the in-memory graph: its neighbors and external ID.
pub struct GraphTerm {
    pub neighbors: AdjacencyList<u32>,
    pub ext_id: i64,
}

// ======================== JniProvider ========================

/// Data provider that keeps the graph in-memory (or lazily loaded) and reads vectors via JNI.
pub struct JniProvider {
    /// In-memory graph: internal_id → { neighbors, ext_id }.
    /// In on-demand mode this acts as a lazy cache — entries are populated on first access.
    graph: DashMap<u32, GraphTerm>,
    /// Internal→External ID mapping (separate for fast lookup).
    int_to_ext: HashMap<u32, i64>,
    /// External→Internal ID mapping.
    ext_to_int: HashMap<i64, u32>,
    /// Start-point IDs and their vectors (always kept in memory).
    start_points: HashMap<u32, Vec<f32>>,
    /// JVM handle for attaching threads.
    jvm: JavaVM,
    /// Global reference to the Java vector reader object (e.g. `FileIOVectorReader`).
    reader_ref: GlobalRef,
    /// Global reference to the Java graph reader object (e.g. `FileIOGraphReader`).
    /// When set, graph neighbors are fetched on demand via JNI callbacks.
    graph_reader_ref: Option<GlobalRef>,
    /// Vector dimension.
    dim: usize,
    /// Distance metric.
    metric: Metric,
    /// Max degree.
    max_degree: usize,
}

impl std::fmt::Debug for JniProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniProvider")
            .field("dim", &self.dim)
            .field("metric", &self.metric)
            .field("max_degree", &self.max_degree)
            .field("graph_size", &self.graph.len())
            .finish()
    }
}

// SAFETY: JavaVM is Send+Sync, GlobalRef is Send+Sync.
unsafe impl Send for JniProvider {}
unsafe impl Sync for JniProvider {}

impl JniProvider {
    /// Build a new search-only provider from pre-constructed graph data.
    ///
    /// * `graph_data`: (internal_id, external_id, neighbors) tuples.
    /// * `start_id`:   internal ID of the graph start point.
    /// * `start_vec`:  vector for the start point (kept in memory).
    /// * `jvm`:        Java VM reference for JNI callbacks.
    /// * `reader_ref`: global ref to the Java vector reader.
    /// * `dim`:        vector dimension.
    /// * `metric_type`:metric enum value (0=L2, 1=IP, 2=Cosine).
    /// * `max_degree`: maximum adjacency list size.
    pub fn new(
        graph_data: Vec<(u32, i64, Vec<u32>)>,
        start_id: u32,
        start_vec: Vec<f32>,
        jvm: JavaVM,
        reader_ref: GlobalRef,
        dim: usize,
        metric_type: i32,
        max_degree: usize,
    ) -> Self {
        let graph = DashMap::new();
        let mut int_to_ext = HashMap::new();
        let mut ext_to_int = HashMap::new();

        for (int_id, ext_id, neighbors) in &graph_data {
            let adj = AdjacencyList::from_iter_untrusted(neighbors.iter().copied());
            graph.insert(
                *int_id,
                GraphTerm {
                    neighbors: adj,
                    ext_id: *ext_id,
                },
            );
            int_to_ext.insert(*int_id, *ext_id);
            ext_to_int.insert(*ext_id, *int_id);
        }

        // Ensure start point is in the graph too.
        if !graph.contains_key(&start_id) {
            graph.insert(
                start_id,
                GraphTerm {
                    neighbors: AdjacencyList::new(),
                    ext_id: start_id as i64,
                },
            );
            int_to_ext.insert(start_id, start_id as i64);
            ext_to_int.insert(start_id as i64, start_id);
        }

        let mut start_points = HashMap::new();
        start_points.insert(start_id, start_vec);

        Self {
            graph,
            int_to_ext,
            ext_to_int,
            start_points,
            jvm,
            reader_ref,
            graph_reader_ref: None,
            dim,
            metric: map_metric(metric_type),
            max_degree,
        }
    }

    /// Build a search-only provider with on-demand graph reading (no pre-loaded graph data).
    ///
    /// The graph `DashMap` starts empty and acts as a lazy cache — entries are populated
    /// on first access via JNI callback to `graphReader.readNeighbors(int)`.
    pub fn new_with_readers(
        int_to_ext: HashMap<u32, i64>,
        ext_to_int: HashMap<i64, u32>,
        start_id: u32,
        start_vec: Vec<f32>,
        jvm: JavaVM,
        reader_ref: GlobalRef,
        graph_reader_ref: GlobalRef,
        dim: usize,
        metric_type: i32,
        max_degree: usize,
    ) -> Self {
        let graph = DashMap::new();

        // The start point needs an entry so `to_internal_id` works.
        // Its neighbors will be fetched on demand.
        let mut start_points = HashMap::new();
        start_points.insert(start_id, start_vec);

        Self {
            graph,
            int_to_ext,
            ext_to_int,
            start_points,
            jvm,
            reader_ref,
            graph_reader_ref: Some(graph_reader_ref),
            dim,
            metric: map_metric(metric_type),
            max_degree,
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Fetch neighbor list from Java via JNI callback to `graphReader.readNeighbors(int)`.
    /// Returns None if graphReader is not set.
    fn fetch_neighbors_jni(&self, int_id: u32) -> Result<Option<Vec<u32>>, JniProviderError> {
        let graph_ref = match &self.graph_reader_ref {
            Some(r) => r,
            None => return Ok(None),
        };

        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| JniProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

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
                return Err(JniProviderError::JniCallFailed(format!(
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
            .map_err(|e| JniProviderError::JniCallFailed(format!("get_array_length: {}", e)))?
            as usize;

        let mut buf = vec![0i32; len];
        env.get_int_array_region(&int_array, 0, &mut buf)
            .map_err(|e| JniProviderError::JniCallFailed(format!("get_int_array_region: {}", e)))?;

        Ok(Some(buf.into_iter().map(|v| v as u32).collect()))
    }

    /// Fetch a vector from Java via JNI.  Returns None if readVector returns null.
    fn fetch_vector_jni(&self, ext_id: i64) -> Result<Option<Vec<f32>>, JniProviderError> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| JniProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

        let result = env.call_method(
            &self.reader_ref,
            "readVector",
            "(J)[F",
            &[jni::objects::JValue::Long(ext_id as jlong)],
        );

        let ret_val = match result {
            Ok(v) => v,
            Err(e) => {
                // Clear any pending Java exception so the JNI env stays usable.
                let _ = env.exception_clear();
                return Err(JniProviderError::JniCallFailed(format!(
                    "readVector({}) failed: {}",
                    ext_id, e
                )));
            }
        };

        let obj = match ret_val.l() {
            Ok(o) => o,
            Err(_) => return Ok(None),
        };

        if obj.is_null() {
            return Ok(None);
        }

        // Convert JFloatArray → Vec<f32>.
        let float_array = jni::objects::JFloatArray::from(obj);
        let len = env
            .get_array_length(&float_array)
            .map_err(|e| JniProviderError::JniCallFailed(format!("get_array_length: {}", e)))?
            as usize;

        let mut buf = vec![0f32; len];
        env.get_float_array_region(&float_array, 0, &mut buf)
            .map_err(|e| {
                JniProviderError::JniCallFailed(format!("get_float_array_region: {}", e))
            })?;

        Ok(Some(buf))
    }
}

// ======================== Context ========================

/// Lightweight execution context (mirrors test_provider::Context).
#[derive(Debug, Clone, Default)]
pub struct JniContext;

impl std::fmt::Display for JniContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "jni context")
    }
}

impl provider::ExecutionContext for JniContext {}

// ======================== DataProvider ========================

impl provider::DataProvider for JniProvider {
    type Context = JniContext;
    type InternalId = u32;
    type ExternalId = u32;
    type Error = JniProviderError;

    fn to_internal_id(
        &self,
        _context: &JniContext,
        gid: &u32,
    ) -> Result<u32, JniProviderError> {
        // Check ID mapping (works for both in-memory and on-demand modes).
        if self.int_to_ext.contains_key(gid) || self.start_points.contains_key(gid) {
            Ok(*gid)
        } else {
            Err(JniProviderError::InvalidId(*gid))
        }
    }

    fn to_external_id(
        &self,
        _context: &JniContext,
        id: u32,
    ) -> Result<u32, JniProviderError> {
        if self.int_to_ext.contains_key(&id) || self.start_points.contains_key(&id) {
            Ok(id)
        } else {
            Err(JniProviderError::InvalidId(id))
        }
    }
}

// ======================== SetElement (stub — search only) ========================

impl provider::SetElement<[f32]> for JniProvider {
    type SetError = ANNError;
    type Guard = provider::NoopGuard<u32>;

    async fn set_element(
        &self,
        _context: &JniContext,
        _id: &u32,
        _element: &[f32],
    ) -> Result<Self::Guard, Self::SetError> {
        Err(ANNError::opaque(JniProviderError::JniCallFailed(
            "set_element not supported on search-only JniProvider".to_string(),
        )))
    }
}

// ======================== NeighborAccessor ========================

#[derive(Debug, Clone, Copy)]
pub struct JniNeighborAccessor<'a> {
    provider: &'a JniProvider,
}

impl provider::HasId for JniNeighborAccessor<'_> {
    type Id = u32;
}

impl provider::NeighborAccessor for JniNeighborAccessor<'_> {
    async fn get_neighbors(
        self,
        id: Self::Id,
        neighbors: &mut AdjacencyList<Self::Id>,
    ) -> ANNResult<Self> {
        // 1. Try in-memory graph (populated upfront or cached from previous JNI calls).
        if let Some(term) = self.provider.graph.get(&id) {
            neighbors.overwrite_trusted(&term.neighbors);
            return Ok(self);
        }

        // 2. On-demand: fetch from Java graph reader via JNI callback.
        if self.provider.graph_reader_ref.is_some() {
            let fetched = self.provider.fetch_neighbors_jni(id)?;
            if let Some(neighbor_ids) = fetched {
                let adj = AdjacencyList::from_iter_untrusted(neighbor_ids.iter().copied());
                neighbors.overwrite_trusted(&adj);
                // Cache in the DashMap for subsequent accesses.
                let ext_id = self.provider.int_to_ext.get(&id).copied().unwrap_or(id as i64);
                self.provider.graph.insert(id, GraphTerm { neighbors: adj, ext_id });
                return Ok(self);
            }
        }

        Err(ANNError::opaque(JniProviderError::InvalidId(id)))
    }
}

impl provider::NeighborAccessorMut for JniNeighborAccessor<'_> {
    async fn set_neighbors(self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<Self> {
        match self.provider.graph.get_mut(&id) {
            Some(mut term) => {
                term.neighbors.clear();
                term.neighbors.extend_from_slice(neighbors);
                Ok(self)
            }
            None => Err(ANNError::opaque(JniProviderError::InvalidId(id))),
        }
    }

    async fn append_vector(self, id: Self::Id, neighbors: &[Self::Id]) -> ANNResult<Self> {
        match self.provider.graph.get_mut(&id) {
            Some(mut term) => {
                term.neighbors.extend_from_slice(neighbors);
                Ok(self)
            }
            None => Err(ANNError::opaque(JniProviderError::InvalidId(id))),
        }
    }
}

// ======================== DefaultAccessor ========================

impl provider::DefaultAccessor for JniProvider {
    type Accessor<'a> = JniNeighborAccessor<'a>;

    fn default_accessor(&self) -> Self::Accessor<'_> {
        JniNeighborAccessor { provider: self }
    }
}

// ======================== Accessor ========================

/// Accessor that fetches vectors via JNI callback.
///
/// Keeps a local buffer and an LRU cache to reduce JNI round-trips.
pub struct JniAccessor<'a> {
    provider: &'a JniProvider,
    buffer: Box<[f32]>,
    cache: VectorCache,
}

impl<'a> JniAccessor<'a> {
    pub fn new(provider: &'a JniProvider, cache_size: usize) -> Self {
        let buffer = vec![0.0f32; provider.dim()].into_boxed_slice();
        Self {
            provider,
            buffer,
            cache: VectorCache::new(cache_size),
        }
    }
}

impl provider::HasId for JniAccessor<'_> {
    type Id = u32;
}

impl provider::Accessor for JniAccessor<'_> {
    type Extended = Box<[f32]>;
    type Element<'a> = &'a [f32] where Self: 'a;
    type ElementRef<'a> = &'a [f32];
    type GetError = JniProviderError;

    async fn get_element(
        &mut self,
        id: u32,
    ) -> Result<Self::Element<'_>, Self::GetError> {
        // 1. Start-point vectors are always in memory.
        if let Some(vec) = self.provider.start_points.get(&id) {
            self.buffer.copy_from_slice(vec);
            return Ok(&*self.buffer);
        }

        // 2. Check LRU cache.
        if let Some(cached) = self.cache.get(id) {
            self.buffer.copy_from_slice(cached);
            return Ok(&*self.buffer);
        }

        // 3. JNI callback to Java: FileIOVectorReader.readVector(extId).
        let ext_id = self
            .provider
            .int_to_ext
            .get(&id)
            .copied()
            .unwrap_or(id as i64);

        let fetched = self.provider.fetch_vector_jni(ext_id)?;

        match fetched {
            Some(vec) if vec.len() == self.provider.dim() => {
                self.buffer.copy_from_slice(&vec);
                self.cache.put(id, vec.into_boxed_slice());
                Ok(&*self.buffer)
            }
            Some(vec) => Err(JniProviderError::JniCallFailed(format!(
                "readVector({}) returned {} floats, expected {}",
                ext_id,
                vec.len(),
                self.provider.dim()
            ))),
            None => Err(JniProviderError::InvalidId(id)),
        }
    }
}

// ======================== DelegateNeighbor ========================

impl<'this> provider::DelegateNeighbor<'this> for JniAccessor<'_> {
    type Delegate = JniNeighborAccessor<'this>;

    fn delegate_neighbor(&'this mut self) -> Self::Delegate {
        JniNeighborAccessor {
            provider: self.provider,
        }
    }
}

// ======================== BuildQueryComputer ========================

impl provider::BuildQueryComputer<[f32]> for JniAccessor<'_> {
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

impl provider::BuildDistanceComputer for JniAccessor<'_> {
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

impl glue::SearchExt for JniAccessor<'_> {
    fn starting_points(
        &self,
    ) -> impl std::future::Future<Output = ANNResult<Vec<u32>>> + Send {
        futures_util::future::ok(self.provider.start_points.keys().copied().collect())
    }
}

// ======================== Blanket traits ========================

impl glue::ExpandBeam<[f32]> for JniAccessor<'_> {}
impl glue::FillSet for JniAccessor<'_> {}

// ======================== Strategy ========================

/// Search-only strategy for the JNI provider.
#[derive(Debug, Default, Clone, Copy)]
pub struct JniStrategy;

impl JniStrategy {
    pub fn new() -> Self {
        Self
    }
}

impl glue::SearchStrategy<JniProvider, [f32]> for JniStrategy {
    type QueryComputer = <f32 as diskann::utils::VectorRepr>::QueryDistance;
    type PostProcessor = glue::CopyIds;
    type SearchAccessorError = diskann::error::Infallible;
    type SearchAccessor<'a> = JniAccessor<'a>;

    fn search_accessor<'a>(
        &'a self,
        provider: &'a JniProvider,
        _context: &'a JniContext,
    ) -> Result<JniAccessor<'a>, diskann::error::Infallible> {
        // Cache up to 1024 recently fetched vectors to reduce JNI round-trips.
        Ok(JniAccessor::new(provider, 1024))
    }

    fn post_processor(&self) -> Self::PostProcessor {
        Default::default()
    }
}

// For insert (graph construction) — delegates to prune/search accessors.
// We implement InsertStrategy and PruneStrategy as stubs since the JniProvider
// is search-only.  DiskANNIndex::new() requires the Provider to be Sized but
// does NOT call insert methods unless we invoke index.insert().
impl glue::PruneStrategy<JniProvider> for JniStrategy {
    type DistanceComputer = <f32 as diskann::utils::VectorRepr>::Distance;
    type PruneAccessor<'a> = JniAccessor<'a>;
    type PruneAccessorError = diskann::error::Infallible;

    fn prune_accessor<'a>(
        &'a self,
        provider: &'a JniProvider,
        _context: &'a JniContext,
    ) -> Result<Self::PruneAccessor<'a>, Self::PruneAccessorError> {
        Ok(JniAccessor::new(provider, 1024))
    }
}

impl glue::InsertStrategy<JniProvider, [f32]> for JniStrategy {
    type PruneStrategy = Self;

    fn prune_strategy(&self) -> Self::PruneStrategy {
        *self
    }

    fn insert_search_accessor<'a>(
        &'a self,
        provider: &'a JniProvider,
        _context: &'a JniContext,
    ) -> Result<Self::SearchAccessor<'a>, Self::SearchAccessorError> {
        Ok(JniAccessor::new(provider, 1024))
    }
}

impl<'a> glue::AsElement<&'a [f32]> for JniAccessor<'a> {
    type Error = diskann::error::Infallible;
    fn as_element(
        &mut self,
        vector: &'a [f32],
        _id: Self::Id,
    ) -> impl std::future::Future<Output = Result<Self::Element<'_>, Self::Error>> + Send {
        std::future::ready(Ok(vector))
    }
}
