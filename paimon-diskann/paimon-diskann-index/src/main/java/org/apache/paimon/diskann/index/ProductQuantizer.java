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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.diskann.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

/**
 * Product Quantization (PQ) implementation for DiskANN.
 *
 * <p>PQ compresses high-dimensional vectors into compact codes by:
 *
 * <ol>
 *   <li>Splitting each vector into {@code M} sub-vectors (subspaces).
 *   <li>Training {@code K} centroids per subspace using K-Means clustering.
 *   <li>Encoding each sub-vector as the index of its nearest centroid (1 byte for K=256).
 * </ol>
 *
 * <p>This produces a "memory thumbnail" — e.g., a 128-dim float vector (512 bytes) is compressed to
 * just 16 bytes (with M=16). During search, the compressed codes stay resident in memory and allow
 * fast approximate distance computation, reducing unnecessary disk I/O for full vectors.
 *
 * <h3>File layout</h3>
 *
 * <p><b>Pivots file</b> ({@code .pq_pivots}): contains the trained codebook.
 *
 * <pre>
 *   int: dimension (D)
 *   int: numSubspaces (M)
 *   int: numCentroids (K)
 *   int: subDimension (D/M)
 *   float[M * K * subDim]: centroid data (row-major)
 * </pre>
 *
 * <p><b>Compressed file</b> ({@code .pq_compressed}): contains the PQ codes.
 *
 * <pre>
 *   int: numVectors (N)
 *   int: numSubspaces (M)
 *   byte[N * M]: PQ codes (row-major, one row per vector)
 * </pre>
 */
public class ProductQuantizer {

    /** Number of centroids per subspace. Fixed at 256 so each code fits in one byte (uint8). */
    public static final int NUM_CENTROIDS = 256;

    /** Default number of K-Means iterations. */
    private static final int DEFAULT_KMEANS_ITERATIONS = 20;

    /** Default maximum number of training samples. */
    private static final int DEFAULT_MAX_TRAINING_SAMPLES = 100_000;

    private final int dimension;
    private final int numSubspaces;
    private final int subDimension;

    /**
     * Codebook: {@code centroids[m][k][d]} is the d-th component of the k-th centroid in the m-th
     * subspace.
     */
    private final float[][][] centroids;

    private ProductQuantizer(int dimension, int numSubspaces, float[][][] centroids) {
        this.dimension = dimension;
        this.numSubspaces = numSubspaces;
        this.subDimension = dimension / numSubspaces;
        this.centroids = centroids;
    }

    // ------------------------------------------------------------------
    // Training
    // ------------------------------------------------------------------

    /**
     * Train a PQ codebook from the given vectors.
     *
     * <p>If there are more vectors than {@code maxTrainingSamples}, a random subset is used for
     * training. All K-Means runs use K-Means++ initialization.
     *
     * @param vectors training data; each entry is a float[dimension]
     * @param dimension vector dimension (must be divisible by {@code numSubspaces})
     * @param numSubspaces number of subspaces M
     * @param maxTrainingSamples maximum number of samples for K-Means training
     * @param kmeansIterations number of Lloyd iterations
     * @return a trained {@link ProductQuantizer}
     */
    public static ProductQuantizer train(
            float[][] vectors,
            int dimension,
            int numSubspaces,
            int maxTrainingSamples,
            int kmeansIterations) {
        if (dimension % numSubspaces != 0) {
            throw new IllegalArgumentException(
                    "Dimension ("
                            + dimension
                            + ") must be divisible by numSubspaces ("
                            + numSubspaces
                            + ")");
        }
        int subDim = dimension / numSubspaces;
        int k = Math.min(NUM_CENTROIDS, vectors.length);
        Random rng = new Random(42);

        // Subsample if needed.
        float[][] samples = vectors;
        if (vectors.length > maxTrainingSamples) {
            samples = sample(vectors, maxTrainingSamples, rng);
        }

        float[][][] codebook = new float[numSubspaces][k][subDim];
        for (int m = 0; m < numSubspaces; m++) {
            // Extract sub-vectors for this subspace.
            float[][] subVectors = extractSubspace(samples, m, subDim);
            codebook[m] = kMeans(subVectors, k, kmeansIterations, rng);
        }
        return new ProductQuantizer(dimension, numSubspaces, codebook);
    }

    // ------------------------------------------------------------------
    // Encoding
    // ------------------------------------------------------------------

    /**
     * Encode a single vector into PQ codes.
     *
     * @param vector float[dimension]
     * @return byte[numSubspaces] — one code per subspace
     */
    public byte[] encode(float[] vector) {
        byte[] codes = new byte[numSubspaces];
        for (int m = 0; m < numSubspaces; m++) {
            int offset = m * subDimension;
            float bestDist = Float.MAX_VALUE;
            int bestIdx = 0;
            for (int k = 0; k < centroids[m].length; k++) {
                float dist = l2Squared(vector, offset, centroids[m][k], 0, subDimension);
                if (dist < bestDist) {
                    bestDist = dist;
                    bestIdx = k;
                }
            }
            codes[m] = (byte) bestIdx;
        }
        return codes;
    }

    /**
     * Encode all vectors.
     *
     * @return byte[numVectors][numSubspaces]
     */
    public byte[][] encodeAll(float[][] vectors) {
        byte[][] codes = new byte[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            codes[i] = encode(vectors[i]);
        }
        return codes;
    }

    // ------------------------------------------------------------------
    // Serialization — Pivots
    // ------------------------------------------------------------------

    /** Serialize the codebook (pivots) to a byte array. */
    public byte[] serializePivots() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(dimension);
        out.writeInt(numSubspaces);
        out.writeInt(centroids[0].length); // K
        out.writeInt(subDimension);
        for (int m = 0; m < numSubspaces; m++) {
            for (int k = 0; k < centroids[m].length; k++) {
                for (int d = 0; d < subDimension; d++) {
                    out.writeFloat(centroids[m][k][d]);
                }
            }
        }
        out.flush();
        return baos.toByteArray();
    }

    // ------------------------------------------------------------------
    // Serialization — Compressed codes
    // ------------------------------------------------------------------

    /**
     * Serialize PQ compressed codes to a byte array.
     *
     * @param codes byte[numVectors][numSubspaces]
     * @param numSubspaces M
     * @return serialized bytes
     */
    public static byte[] serializeCompressed(byte[][] codes, int numSubspaces) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeInt(codes.length);
        out.writeInt(numSubspaces);
        for (byte[] code : codes) {
            out.write(code);
        }
        out.flush();
        return baos.toByteArray();
    }

    /**
     * Compute a reasonable default number of subspaces for the given dimension. The result is the
     * largest divisor of {@code dim} that is {@code <= dim / 4} and at least 1.
     */
    public static int defaultNumSubspaces(int dim) {
        int target = Math.max(1, dim / 4);
        while (target > 1 && dim % target != 0) {
            target--;
        }
        return target;
    }

    // ------------------------------------------------------------------
    // K-Means (K-Means++ init + Lloyd iterations)
    // ------------------------------------------------------------------

    /**
     * Run K-Means clustering on the given data.
     *
     * @param data float[n][subDim]
     * @param k number of clusters
     * @param maxIter maximum iterations
     * @param rng random source
     * @return float[k][subDim] — cluster centroids
     */
    static float[][] kMeans(float[][] data, int k, int maxIter, Random rng) {
        int n = data.length;
        int d = data[0].length;

        if (n <= k) {
            // Fewer data points than clusters — each point is its own centroid.
            float[][] centroids = new float[k][d];
            for (int i = 0; i < n; i++) {
                centroids[i] = Arrays.copyOf(data[i], d);
            }
            for (int i = n; i < k; i++) {
                centroids[i] = Arrays.copyOf(data[rng.nextInt(n)], d);
            }
            return centroids;
        }

        // K-Means++ initialization.
        float[][] centroids = kMeansPPInit(data, k, rng);

        int[] assignments = new int[n];
        int[] counts = new int[k];

        for (int iter = 0; iter < maxIter; iter++) {
            // Assignment step.
            for (int i = 0; i < n; i++) {
                float bestDist = Float.MAX_VALUE;
                int bestC = 0;
                for (int c = 0; c < k; c++) {
                    float dist = l2Squared(data[i], 0, centroids[c], 0, d);
                    if (dist < bestDist) {
                        bestDist = dist;
                        bestC = c;
                    }
                }
                assignments[i] = bestC;
            }

            // Update step.
            float[][] newCentroids = new float[k][d];
            Arrays.fill(counts, 0);
            for (int i = 0; i < n; i++) {
                int c = assignments[i];
                counts[c]++;
                for (int j = 0; j < d; j++) {
                    newCentroids[c][j] += data[i][j];
                }
            }
            for (int c = 0; c < k; c++) {
                if (counts[c] > 0) {
                    for (int j = 0; j < d; j++) {
                        newCentroids[c][j] /= counts[c];
                    }
                } else {
                    // Re-seed empty cluster from a random data point.
                    newCentroids[c] = Arrays.copyOf(data[rng.nextInt(n)], d);
                }
            }
            centroids = newCentroids;
        }
        return centroids;
    }

    /** K-Means++ initialization: pick k initial centroids with probability proportional to D^2. */
    private static float[][] kMeansPPInit(float[][] data, int k, Random rng) {
        int n = data.length;
        int d = data[0].length;
        float[][] centroids = new float[k][d];

        // Pick first centroid uniformly at random.
        centroids[0] = Arrays.copyOf(data[rng.nextInt(n)], d);

        float[] minDist = new float[n];
        Arrays.fill(minDist, Float.MAX_VALUE);

        for (int c = 1; c < k; c++) {
            // Update minimum distances to chosen centroids.
            float totalDist = 0f;
            for (int i = 0; i < n; i++) {
                float dist = l2Squared(data[i], 0, centroids[c - 1], 0, d);
                if (dist < minDist[i]) {
                    minDist[i] = dist;
                }
                totalDist += minDist[i];
            }

            // Sample next centroid proportional to D^2.
            float threshold = rng.nextFloat() * totalDist;
            float cumulative = 0f;
            int chosen = n - 1;
            for (int i = 0; i < n; i++) {
                cumulative += minDist[i];
                if (cumulative >= threshold) {
                    chosen = i;
                    break;
                }
            }
            centroids[c] = Arrays.copyOf(data[chosen], d);
        }
        return centroids;
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Compute L2 squared distance between two sub-arrays. */
    private static float l2Squared(float[] a, int offsetA, float[] b, int offsetB, int length) {
        float sum = 0f;
        for (int i = 0; i < length; i++) {
            float diff = a[offsetA + i] - b[offsetB + i];
            sum += diff * diff;
        }
        return sum;
    }

    /** Random sampling without replacement (Fisher-Yates on indices). */
    private static float[][] sample(float[][] data, int sampleSize, Random rng) {
        int n = data.length;
        int[] indices = new int[n];
        for (int i = 0; i < n; i++) {
            indices[i] = i;
        }
        for (int i = 0; i < sampleSize; i++) {
            int j = i + rng.nextInt(n - i);
            int tmp = indices[i];
            indices[i] = indices[j];
            indices[j] = tmp;
        }
        float[][] result = new float[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            result[i] = data[indices[i]];
        }
        return result;
    }

    /** Extract the m-th subspace from a set of vectors. */
    private static float[][] extractSubspace(float[][] vectors, int m, int subDim) {
        int offset = m * subDim;
        float[][] sub = new float[vectors.length][subDim];
        for (int i = 0; i < vectors.length; i++) {
            System.arraycopy(vectors[i], offset, sub[i], 0, subDim);
        }
        return sub;
    }
}
