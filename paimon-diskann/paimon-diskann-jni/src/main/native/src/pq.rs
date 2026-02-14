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

//! Product Quantization (PQ) for DiskANN, backed by `diskann-quantization`.
//!
//! PQ compresses high-dimensional vectors into compact codes by:
//! 1. Splitting each vector into `M` sub-vectors (subspaces).
//! 2. Training `K` centroids per subspace using K-Means clustering
//!    (via `diskann-quantization`'s `LightPQTrainingParameters`).
//! 3. Encoding each sub-vector as the index of its nearest centroid (1 byte for K≤256).

use diskann_quantization::cancel::DontCancel;
use diskann_quantization::product::train::{LightPQTrainingParameters, TrainQuantizer};
use diskann_quantization::product::BasicTable;
use diskann_quantization::random::StdRngBuilder;
use diskann_quantization::views::ChunkOffsetsView;
use diskann_quantization::{CompressInto, Parallelism};
use diskann_utils::views::Matrix;

/// Maximum centroids per subspace.  Fixed at 256 so each code fits in one byte (u8).
const NUM_CENTROIDS: usize = 256;

/// Result of PQ training and encoding.
#[derive(Debug)]
pub struct PQResult {
    /// Serialized PQ codebook (pivots).
    pub pivots_bytes: Vec<u8>,
    /// Serialized compressed PQ codes.
    pub compressed_bytes: Vec<u8>,
}

/// Train a PQ codebook and encode all vectors using `diskann-quantization`.
///
/// * `vectors`        — training vectors (stored sequentially, position = ID).
/// * `dimension`      — vector dimension (must be divisible by `num_subspaces`).
/// * `num_subspaces`  — number of PQ subspaces (M).
/// * `max_samples`    — maximum number of vectors sampled for training.
/// * `kmeans_iters`   — number of Lloyd iterations for K-Means.
///
/// Returns serialized pivots and compressed codes.
pub fn train_and_encode(
    vectors: &[Vec<f32>],
    dimension: usize,
    num_subspaces: usize,
    max_samples: usize,
    kmeans_iters: usize,
) -> Result<PQResult, String> {
    if dimension == 0 || num_subspaces == 0 || dimension % num_subspaces != 0 {
        return Err(format!(
            "Invalid PQ params: dim={}, num_subspaces={}",
            dimension, num_subspaces
        ));
    }

    let n = vectors.len();
    if n == 0 {
        return Err("No vectors to train PQ".to_string());
    }

    let sub_dim = dimension / num_subspaces;
    let k = std::cmp::min(NUM_CENTROIDS, n);

    // --- Build training data as a Matrix<f32> (nrows=num_samples, ncols=dim) ---
    let sample_n = std::cmp::min(n, max_samples);
    let sample_indices = if n > max_samples {
        sample_indices_det(n, max_samples)
    } else {
        (0..n).collect()
    };

    let mut training_data = Matrix::new(0.0f32, sample_n, dimension);
    for (dst_row, &src_idx) in sample_indices.iter().enumerate() {
        training_data
            .row_mut(dst_row)
            .copy_from_slice(&vectors[src_idx]);
    }

    // --- Build chunk offsets (uniform subspaces) ---
    // e.g. for dim=8, M=2: offsets = [0, 4, 8]
    let offsets: Vec<usize> = (0..=num_subspaces).map(|i| i * sub_dim).collect();
    let schema = ChunkOffsetsView::new(&offsets)
        .map_err(|e| format!("Failed to create PQ chunk offsets: {}", e))?;

    // --- Train using diskann-quantization ---
    let trainer = LightPQTrainingParameters::new(k, kmeans_iters);
    let rng_builder = StdRngBuilder::new(42);

    let quantizer = trainer
        .train(
            training_data.as_view(),
            schema,
            Parallelism::Sequential,
            &rng_builder,
            &DontCancel,
        )
        .map_err(|e| format!("PQ training failed: {}", e))?;

    // --- Build BasicTable for encoding ---
    let ncenters = quantizer.pivots()[0].nrows();
    let flat_pivots: Vec<f32> = quantizer.flatten();
    let pivots_matrix = Matrix::try_from(flat_pivots.into_boxed_slice(), ncenters, dimension)
        .map_err(|e| format!("Failed to create pivot matrix: {}", e))?;

    let offsets_owned = schema.to_owned();
    let table = BasicTable::new(pivots_matrix, offsets_owned)
        .map_err(|e| format!("Failed to create BasicTable: {}", e))?;

    // --- Encode all vectors ---
    let mut all_codes: Vec<Vec<u8>> = Vec::with_capacity(n);
    for vec in vectors.iter() {
        let mut code = vec![0u8; num_subspaces];
        table
            .compress_into(vec.as_slice(), &mut code)
            .map_err(|e| format!("PQ compression failed: {}", e))?;
        all_codes.push(code);
    }

    // --- Serialize ---
    let pivots_bytes = serialize_pivots(&table, dimension, num_subspaces, ncenters, sub_dim);
    let compressed_bytes = serialize_compressed(&all_codes, num_subspaces);

    Ok(PQResult {
        pivots_bytes,
        compressed_bytes,
    })
}

/// Serialize the PQ codebook (pivots) to bytes.
///
/// Format (native byte order):
/// ```text
/// i32: dimension
/// i32: num_subspaces (M)
/// i32: num_centroids (K)
/// i32: sub_dimension
/// f32[M * K * sub_dim]: centroid data (stored per-subspace)
/// ```
fn serialize_pivots(
    table: &BasicTable,
    dimension: usize,
    num_subspaces: usize,
    num_centroids: usize,
    sub_dim: usize,
) -> Vec<u8> {
    // The pivots in BasicTable are stored row-major: ncenters rows × dim columns.
    // Each row has all subspaces concatenated.
    // We need to serialize in the per-subspace format expected by the reader:
    // for subspace m: for centroid k: float[sub_dim]
    let header_size = 4 * 4;
    let data_size = num_subspaces * num_centroids * sub_dim * 4;
    let mut buf = Vec::with_capacity(header_size + data_size);

    buf.extend_from_slice(&(dimension as i32).to_ne_bytes());
    buf.extend_from_slice(&(num_subspaces as i32).to_ne_bytes());
    buf.extend_from_slice(&(num_centroids as i32).to_ne_bytes());
    buf.extend_from_slice(&(sub_dim as i32).to_ne_bytes());

    let pivots_view = table.view_pivots();
    // Reorder from row-major (centroid × full_dim) to subspace-major:
    // subspace m, centroid k → row k, columns [m*sub_dim .. (m+1)*sub_dim]
    for m in 0..num_subspaces {
        let col_start = m * sub_dim;
        for k in 0..num_centroids {
            let row = pivots_view.row(k);
            for d in 0..sub_dim {
                buf.extend_from_slice(&row[col_start + d].to_ne_bytes());
            }
        }
    }

    buf
}

/// Serialize compressed PQ codes to bytes.
///
/// Format (native byte order):
/// ```text
/// i32: num_vectors (N)
/// i32: num_subspaces (M)
/// byte[N * M]: PQ codes
/// ```
fn serialize_compressed(codes: &[Vec<u8>], num_subspaces: usize) -> Vec<u8> {
    let header_size = 4 * 2;
    let data_size = codes.len() * num_subspaces;
    let mut buf = Vec::with_capacity(header_size + data_size);

    buf.extend_from_slice(&(codes.len() as i32).to_ne_bytes());
    buf.extend_from_slice(&(num_subspaces as i32).to_ne_bytes());

    for code in codes {
        buf.extend_from_slice(code);
    }
    buf
}

/// Deterministic sampling without replacement (Fisher-Yates on indices).
fn sample_indices_det(n: usize, sample_size: usize) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..n).collect();
    let mut rng = SimpleRng::new(42);

    let m = std::cmp::min(sample_size, n);
    for i in 0..m {
        let j = i + rng.next_usize(n - i);
        indices.swap(i, j);
    }
    indices.truncate(m);
    indices
}

/// Minimal deterministic PRNG (xorshift64).
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { 1 } else { seed },
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pq_train_encode_roundtrip() {
        let dim = 8;
        let num_subspaces = 2;
        let n = 50;

        // Generate some simple vectors.
        let vectors: Vec<Vec<f32>> = (0..n)
            .map(|i| {
                (0..dim).map(|d| (i * dim + d) as f32 * 0.01).collect()
            })
            .collect();

        let result = train_and_encode(&vectors, dim, num_subspaces, 100, 5).unwrap();

        // Check pivots serialization.
        assert!(result.pivots_bytes.len() > 16);

        // Verify header: dim=8, M=2, K=min(256,50)=50, sub_dim=4
        let dim_read = i32::from_ne_bytes(result.pivots_bytes[0..4].try_into().unwrap());
        let m_read = i32::from_ne_bytes(result.pivots_bytes[4..8].try_into().unwrap());
        let k_read = i32::from_ne_bytes(result.pivots_bytes[8..12].try_into().unwrap());
        let sub_dim_read = i32::from_ne_bytes(result.pivots_bytes[12..16].try_into().unwrap());
        assert_eq!(dim_read, 8);
        assert_eq!(m_read, 2);
        assert_eq!(k_read, 50);
        assert_eq!(sub_dim_read, 4);

        // Check expected pivots size: 16 header + 2*50*4*4 = 16 + 1600 = 1616
        assert_eq!(result.pivots_bytes.len(), 16 + 2 * 50 * 4 * 4);

        // Check compressed serialization.
        assert!(result.compressed_bytes.len() > 8);

        // Verify compressed header: N=50, M=2
        let n_read = i32::from_ne_bytes(result.compressed_bytes[0..4].try_into().unwrap());
        let m_comp_read = i32::from_ne_bytes(result.compressed_bytes[4..8].try_into().unwrap());
        assert_eq!(n_read, 50);
        assert_eq!(m_comp_read, 2);

        // Check expected compressed size: 8 header + 50*2 = 108
        assert_eq!(result.compressed_bytes.len(), 8 + 50 * 2);
    }

    #[test]
    fn test_pq_invalid_params() {
        let vectors: Vec<Vec<f32>> = vec![vec![1.0, 2.0, 3.0, 4.0]];

        // dim not divisible by num_subspaces
        let err = train_and_encode(&vectors, 4, 3, 100, 5).unwrap_err();
        assert!(err.contains("Invalid PQ params"));

        // num_subspaces = 0
        let err = train_and_encode(&vectors, 4, 0, 100, 5).unwrap_err();
        assert!(err.contains("Invalid PQ params"));

        // empty vectors
        let err = train_and_encode(&[], 4, 2, 100, 5).unwrap_err();
        assert!(err.contains("No vectors"));
    }
}
