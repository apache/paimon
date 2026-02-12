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
//!  - **Vectors**: read through `FileIOVectorReader.readVector(long)`, which
//!    reads from a `SeekableInputStream` over the `.data` file.
//!
//! Frequently accessed neighbors and vectors are cached in a `DashMap` and
//! an LRU cache respectively, to reduce FileIO/JNI round-trips.

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

// ======================== LRU Cache ========================

/// Tiny LRU cache for recently fetched vectors to reduce FileIO/JNI round-trips.
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

/// One entry in the graph cache: its neighbor list.
pub struct GraphTerm {
    pub neighbors: AdjacencyList<u32>,
}

// ======================== FileIOProvider ========================

/// DiskANN data provider backed by Paimon FileIO.
///
/// Graph neighbors and vectors are read on demand from FileIO-backed storage
/// (local, HDFS, S3, OSS, etc.) via JNI callbacks to Java reader objects.
/// A `DashMap` lazily caches graph entries to reduce repeated FileIO reads.
pub struct FileIOProvider {
    /// Graph cache: internal_id → { neighbors }.
    /// Acts as a lazy cache — entries are populated on first access from FileIO.
    graph: DashMap<u32, GraphTerm>,
    /// Total number of nodes (start point + user vectors).
    num_nodes: usize,
    /// Start-point IDs and their vectors (always kept in memory).
    start_points: HashMap<u32, Vec<f32>>,
    /// JVM handle for attaching threads.
    jvm: JavaVM,
    /// Global reference to the Java vector reader object (`FileIOVectorReader`).
    reader_ref: GlobalRef,
    /// Global reference to the Java graph reader object (`FileIOGraphReader`).
    /// When set, graph neighbors are fetched on demand via JNI callbacks.
    graph_reader_ref: Option<GlobalRef>,
    /// Vector dimension.
    dim: usize,
    /// Distance metric.
    metric: Metric,
    /// Max degree.
    max_degree: usize,
}

impl std::fmt::Debug for FileIOProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileIOProvider")
            .field("dim", &self.dim)
            .field("metric", &self.metric)
            .field("max_degree", &self.max_degree)
            .field("graph_size", &self.graph.len())
            .finish()
    }
}

// SAFETY: JavaVM is Send+Sync, GlobalRef is Send+Sync.
unsafe impl Send for FileIOProvider {}
unsafe impl Sync for FileIOProvider {}

impl FileIOProvider {
    /// Build a search-only provider with on-demand graph reading (no pre-loaded graph data).
    ///
    /// The graph `DashMap` starts empty and acts as a lazy cache — entries are populated
    /// on first access via FileIO through `graphReader.readNeighbors(int)`.
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
    ) -> Self {
        let graph = DashMap::new();

        let mut start_points = HashMap::new();
        start_points.insert(start_id, start_vec);

        Self {
            graph,
            num_nodes,
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

    /// Fetch neighbor list from FileIO-backed storage via JNI callback to
    /// `graphReader.readNeighbors(int)`.  Returns None if graphReader is not set.
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

    /// Fetch a vector from FileIO-backed storage via JNI callback to
    /// `vectorReader.readVector(long)`.  The `position` is the 0-based index
    /// in the data file (position = int_id - 1).
    fn fetch_vector(&self, position: i64) -> Result<Option<Vec<f32>>, FileIOProviderError> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("attach failed: {}", e)))?;

        let result = env.call_method(
            &self.reader_ref,
            "readVector",
            "(J)[F",
            &[jni::objects::JValue::Long(position as jlong)],
        );

        let ret_val = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = env.exception_clear();
                return Err(FileIOProviderError::JniCallFailed(format!(
                    "readVector({}) failed: {}",
                    position, e
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
            .map_err(|e| FileIOProviderError::JniCallFailed(format!("get_array_length: {}", e)))?
            as usize;

        let mut buf = vec![0f32; len];
        env.get_float_array_region(&float_array, 0, &mut buf)
            .map_err(|e| {
                FileIOProviderError::JniCallFailed(format!("get_float_array_region: {}", e))
            })?;

        Ok(Some(buf))
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
        // 1. Try cached graph (populated upfront or cached from previous FileIO reads).
        if let Some(term) = self.provider.graph.get(&id) {
            neighbors.overwrite_trusted(&term.neighbors);
            return Ok(self);
        }

        // 2. On-demand: fetch from FileIO-backed storage via graph reader JNI callback.
        if self.provider.graph_reader_ref.is_some() {
            let fetched = self.provider.fetch_neighbors(id)?;
            if let Some(neighbor_ids) = fetched {
                let adj = AdjacencyList::from_iter_untrusted(neighbor_ids.iter().copied());
                neighbors.overwrite_trusted(&adj);
                // Cache in the DashMap for subsequent accesses.
                self.provider.graph.insert(id, GraphTerm { neighbors: adj });
                return Ok(self);
            }
        }

        Err(ANNError::opaque(FileIOProviderError::InvalidId(id)))
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

/// Accessor that fetches vectors from FileIO-backed storage via JNI callback.
///
/// Keeps a local buffer and an LRU cache to reduce FileIO/JNI round-trips.
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

        // 2. Check LRU cache.
        if let Some(cached) = self.cache.get(id) {
            self.buffer.copy_from_slice(cached);
            return Ok(&*self.buffer);
        }

        // 3. Fetch from FileIO-backed storage via FileIOVectorReader.readVector(position).
        //    position = int_id - 1 (start point is int_id=0, user vectors start at 1).
        let position = (id as i64) - 1;

        let fetched = self.provider.fetch_vector(position)?;

        match fetched {
            Some(vec) if vec.len() == self.provider.dim() => {
                self.buffer.copy_from_slice(&vec);
                self.cache.put(id, vec.into_boxed_slice());
                Ok(&*self.buffer)
            }
            Some(vec) => Err(FileIOProviderError::JniCallFailed(format!(
                "readVector({}) returned {} floats, expected {}",
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
        // Cache up to 1024 recently fetched vectors to reduce FileIO round-trips.
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
