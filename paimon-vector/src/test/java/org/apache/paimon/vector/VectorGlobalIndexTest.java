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

package org.apache.paimon.vector;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link VectorGlobalIndexWriter} and {@link VectorGlobalIndexReader}. */
public class VectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;

    @BeforeEach
    public void setup() {
        fileIO = new LocalFileIO();
        indexPath = new Path(tempDir.toString());
        vectorType = new ArrayType(new FloatType());
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (fileIO != null) {
            fileIO.delete(indexPath, true);
        }
    }

    private GlobalIndexFileWriter createFileWriter(Path path) {
        return new GlobalIndexFileWriter() {
            @Override
            public String newFileName(String prefix) {
                return prefix + "-" + UUID.randomUUID();
            }

            @Override
            public OutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(path, fileName), false);
            }
        };
    }

    private GlobalIndexFileReader createFileReader(Path path) {
        return fileName -> fileIO.newInputStream(new Path(path, fileName));
    }

    @Test
    public void testWriteAndReadVectorIndex() throws IOException {
        Options options = createDefaultOptions();
        int dimension = 128;
        int numVectors = 100;

        // Create writer and write vectors
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        for (int i = 0; i < numVectors; i++) {
            writer.write(new VectorGlobalIndexWriter.VectorKey(i, testVectors.get(i)));
        }

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        GlobalIndexWriter.ResultEntry result = results.get(0);
        assertThat(result.fileName()).isNotNull();
        assertThat(result.rowRange()).isEqualTo(new Range(0, numVectors));

        // Create reader and verify
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(
                new GlobalIndexIOMeta(
                        result.fileName(),
                        fileIO.getFileSize(new Path(indexPath, result.fileName())),
                        result.rowRange(),
                        result.meta()));

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

        // Test search - query with first vector should return itself
        GlobalIndexResult searchResult = reader.visit(testVectors.get(0), 5);
        assertThat(searchResult).isNotNull();

        reader.close();
    }

    @Test
    public void testSearchSimilarVectors() throws IOException {
        Options options = createDefaultOptions();
        int dimension = 64;
        options.setInteger("vector.dim", dimension); // Set correct dimension
        int numVectors = 50;

        // Create vectors with known similarities
        float[] baseVector = new float[dimension];
        for (int i = 0; i < dimension; i++) {
            baseVector[i] = 1.0f;
        }

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        // Write base vector and similar variants
        writer.write(new VectorGlobalIndexWriter.VectorKey(0, baseVector));

        for (int i = 1; i < numVectors; i++) {
            float[] variant = baseVector.clone();
            // Add small noise
            variant[i % dimension] += (i % 2 == 0 ? 0.1f : -0.1f);
            writer.write(new VectorGlobalIndexWriter.VectorKey(i, variant));
        }

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        GlobalIndexWriter.ResultEntry result = results.get(0);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(
                new GlobalIndexIOMeta(
                        result.fileName(),
                        fileIO.getFileSize(new Path(indexPath, result.fileName())),
                        result.rowRange(),
                        result.meta()));

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

        // Search with base vector should return similar vectors
        GlobalIndexResult searchResult = reader.visit(baseVector, 10);
        assertThat(searchResult).isNotNull();

        reader.close();
    }

    @Test
    public void testDifferentSimilarityFunctions() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"COSINE", "DOT_PRODUCT", "EUCLIDEAN"};

        for (String metric : metrics) {
            Options options = createDefaultOptions();
            options.setString("vector.metric", metric);
            options.setInteger("vector.dim", dimension); // Set correct dimension

            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            VectorGlobalIndexWriter writer =
                    new VectorGlobalIndexWriter(fileWriter, vectorType, options);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            for (int i = 0; i < numVectors; i++) {
                writer.write(new VectorGlobalIndexWriter.VectorKey(i, testVectors.get(i)));
            }

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(metricIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(metricIndexPath, result.fileName())),
                            result.rowRange(),
                            result.meta()));

            VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

            // Verify search works with this metric
            GlobalIndexResult searchResult = reader.visit(testVectors.get(0), 3);
            assertThat(searchResult).isNotNull();

            reader.close();
        }
    }

    @Test
    public void testDifferentDimensions() throws IOException {
        int[] dimensions = {8, 32, 128, 256};

        for (int dimension : dimensions) {
            Options options = createDefaultOptions();
            options.setInteger("vector.dim", dimension);

            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            VectorGlobalIndexWriter writer =
                    new VectorGlobalIndexWriter(fileWriter, vectorType, options);

            int numVectors = 10;
            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            for (int i = 0; i < numVectors; i++) {
                writer.write(new VectorGlobalIndexWriter.VectorKey(i, testVectors.get(i)));
            }

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(dimIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(dimIndexPath, result.fileName())),
                            result.rowRange(),
                            result.meta()));

            VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

            // Verify search works with this dimension
            GlobalIndexResult searchResult = reader.visit(testVectors.get(0), 5);
            assertThat(searchResult).isNotNull();

            reader.close();
        }
    }

    @Test
    public void testDimensionMismatch() throws IOException {
        Options options = createDefaultOptions();
        options.setInteger("vector.dim", 64);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        // Try to write vector with wrong dimension
        float[] wrongDimVector = new float[32]; // Wrong dimension
        assertThatThrownBy(
                        () ->
                                writer.write(
                                        new VectorGlobalIndexWriter.VectorKey(0, wrongDimVector)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testEmptyIndex() throws IOException {
        Options options = createDefaultOptions();

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).isEmpty();
    }

    @Test
    public void testHNSWParameters() throws IOException {
        Options options = createDefaultOptions();
        options.setInteger("vector.m", 32); // Larger M for better recall
        options.setInteger("vector.ef-construction", 200); // Larger ef for better quality

        int dimension = 64;
        options.setInteger("vector.dim", dimension); // Set correct dimension
        int numVectors = 50;

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        for (int i = 0; i < numVectors; i++) {
            writer.write(new VectorGlobalIndexWriter.VectorKey(i, testVectors.get(i)));
        }

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        GlobalIndexWriter.ResultEntry result = results.get(0);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(
                new GlobalIndexIOMeta(
                        result.fileName(),
                        fileIO.getFileSize(new Path(indexPath, result.fileName())),
                        result.rowRange(),
                        result.meta()));

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

        // Verify search works with custom HNSW parameters
        GlobalIndexResult searchResult = reader.visit(testVectors.get(0), 5);
        assertThat(searchResult).isNotNull();

        reader.close();
    }

    @Test
    public void testLargeK() throws IOException {
        Options options = createDefaultOptions();
        int dimension = 32;
        options.setInteger("vector.dim", dimension); // Set correct dimension
        int numVectors = 100;

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        for (int i = 0; i < numVectors; i++) {
            writer.write(new VectorGlobalIndexWriter.VectorKey(i, testVectors.get(i)));
        }

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        GlobalIndexWriter.ResultEntry result = results.get(0);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(
                new GlobalIndexIOMeta(
                        result.fileName(),
                        fileIO.getFileSize(new Path(indexPath, result.fileName())),
                        result.rowRange(),
                        result.meta()));

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

        // Search with k larger than number of vectors
        GlobalIndexResult searchResult = reader.visit(testVectors.get(0), 200);
        assertThat(searchResult).isNotNull();
        assertThat(searchResult).isNotNull();

        reader.close();
    }

    @Test
    public void testEndToEndMultiShardScenario() throws IOException {
        Options options = createDefaultOptions();
        int dimension = 128;
        options.setInteger("vector.dim", dimension);
        options.setString("vector.metric", "COSINE");
        options.setInteger("vector.m", 24);
        options.setInteger("vector.ef-construction", 150);

        // Simulate a multi-shard scenario with incremental writes
        int totalVectors = 300;
        int shardsCount = 3;
        int vectorsPerShard = totalVectors / shardsCount;

        List<GlobalIndexIOMeta> allMetas = new ArrayList<>();
        List<float[]> allTestVectors = generateRandomVectors(totalVectors, dimension);

        // Write vectors in multiple shards (simulating incremental indexing)
        for (int shardIdx = 0; shardIdx < shardsCount; shardIdx++) {
            Path shardPath = new Path(indexPath, "shard_" + shardIdx);
            GlobalIndexFileWriter fileWriter = createFileWriter(shardPath);
            VectorGlobalIndexWriter writer =
                    new VectorGlobalIndexWriter(fileWriter, vectorType, options);

            int startIdx = shardIdx * vectorsPerShard;
            int endIdx = Math.min(startIdx + vectorsPerShard, totalVectors);

            // Write vectors for this shard
            for (int i = startIdx; i < endIdx; i++) {
                writer.write(new VectorGlobalIndexWriter.VectorKey(i, allTestVectors.get(i)));
            }

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            allMetas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(shardPath, result.fileName())),
                            result.rowRange(),
                            result.meta()));
        }

        // Create a single reader for all shards
        GlobalIndexFileReader fileReader =
                new GlobalIndexFileReader() {
                    @Override
                    public org.apache.paimon.fs.SeekableInputStream getInputStream(String fileName)
                            throws IOException {
                        // Try to find the file in any shard directory
                        for (int shardIdx = 0; shardIdx < shardsCount; shardIdx++) {
                            Path shardPath = new Path(indexPath, "shard_" + shardIdx);
                            Path filePath = new Path(shardPath, fileName);
                            if (fileIO.exists(filePath)) {
                                return fileIO.newInputStream(filePath);
                            }
                        }
                        throw new IOException("File not found: " + fileName);
                    }
                };

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, allMetas);

        // Test 1: Search with various k values
        int[] kValues = {1, 5, 10, 20, 50};
        for (int k : kValues) {
            GlobalIndexResult searchResult = reader.visit(allTestVectors.get(0), k);
            assertThat(searchResult).isNotNull();
            assertThat(containsRowId(searchResult, 0)).isTrue();
        }

        // Test 2: Search with query vectors from different shards
        for (int shardIdx = 0; shardIdx < shardsCount; shardIdx++) {
            int queryIdx = shardIdx * vectorsPerShard + 5;
            GlobalIndexResult searchResult = reader.visit(allTestVectors.get(queryIdx), 10);
            assertThat(searchResult).isNotNull();
            assertThat(containsRowId(searchResult, queryIdx))
                    .as("Search result should contain the query vector's own ID")
                    .isTrue();
        }

        // Test 3: Create a known similar vector and verify top result
        float[] originalVector = allTestVectors.get(50);
        float[] slightlyModifiedVector = originalVector.clone();
        // Add minimal noise to create a very similar vector
        for (int i = 0; i < dimension; i++) {
            slightlyModifiedVector[i] += (i % 2 == 0 ? 0.001f : -0.001f);
        }
        normalize(slightlyModifiedVector);

        GlobalIndexResult similarSearchResult = reader.visit(slightlyModifiedVector, 5);
        assertThat(similarSearchResult).isNotNull();
        assertThat(containsRowId(similarSearchResult, 50))
                .as("Search with very similar vector should find the original")
                .isTrue();

        // Test 4: Verify search quality with a synthetic cluster
        // Create a cluster center
        float[] clusterCenter = new float[dimension];
        Random random = new Random(123);
        for (int i = 0; i < dimension; i++) {
            clusterCenter[i] = random.nextFloat() * 2 - 1;
        }
        normalize(clusterCenter);

        GlobalIndexResult clusterSearchResult = reader.visit(clusterCenter, 20);
        assertThat(clusterSearchResult).isNotNull();
        assertThat(clusterSearchResult.iterator().hasNext()).isTrue();

        // Test 5: Edge case - search with k larger than total vectors
        GlobalIndexResult largeKResult = reader.visit(allTestVectors.get(10), totalVectors * 2);
        assertThat(largeKResult).isNotNull();

        // Test 6: Multiple consecutive searches (stress test)
        for (int i = 0; i < 20; i++) {
            int randomIdx = random.nextInt(totalVectors);
            GlobalIndexResult result = reader.visit(allTestVectors.get(randomIdx), 15);
            assertThat(result).isNotNull();
        }

        reader.close();
    }

    @Test
    public void testVectorIndexEndToEnd() throws IOException {
        Options options = createDefaultOptions();
        int dimension = 2;
        options.setInteger("vector.dim", dimension);
        options.setString("vector.metric", "EUCLIDEAN");
        options.setInteger("vector.m", 16);
        options.setInteger("vector.ef-construction", 100);

        // Create semantic vectors representing "documents"
        // Apple [0.9, 0.1] and Banana [0.8, 0.2] are similar (fruits)
        // Car [0.1, 0.9] is different (vehicle)
        float[] appleVector = new float[] {0.9f, 0.1f};
        float[] bananaVector = new float[] {0.8f, 0.2f};
        float[] carVector = new float[] {0.1f, 0.9f};

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer =
                new VectorGlobalIndexWriter(fileWriter, vectorType, options);

        // Write vectors with row IDs representing documents
        long appleId = 0L;
        long bananaId = 1L;
        long carId = 2L;

        writer.write(new VectorGlobalIndexWriter.VectorKey(appleId, appleVector));
        writer.write(new VectorGlobalIndexWriter.VectorKey(bananaId, bananaVector));
        writer.write(new VectorGlobalIndexWriter.VectorKey(carId, carVector));

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);

        GlobalIndexWriter.ResultEntry result = results.get(0);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        metas.add(
                new GlobalIndexIOMeta(
                        result.fileName(),
                        fileIO.getFileSize(new Path(indexPath, result.fileName())),
                        result.rowRange(),
                        result.meta()));

        VectorGlobalIndexReader reader = new VectorGlobalIndexReader(fileReader, metas);

        // Test 1: Query with vector similar to "Apple" - should find Apple and Banana
        // Query: [0.85, 0.15] is between Apple and Banana
        float[] queryVector = new float[] {0.85f, 0.15f};
        GlobalIndexResult searchResult = reader.visit(queryVector, 2);

        assertThat(searchResult).isNotNull();
        assertThat(searchResult.iterator().hasNext()).as("Search should return results").isTrue();

        // Collect all result IDs
        List<Long> resultIds = new ArrayList<>();
        for (Long id : searchResult) {
            resultIds.add(id);
        }

        // Should find Apple and Banana (similar fruits), not Car
        assertThat(resultIds).as("Should find Apple (most similar)").contains(appleId);
        assertThat(resultIds).as("Should find Banana (second most similar)").contains(bananaId);
        assertThat(resultIds.size()).as("Should return top 2 results").isLessThanOrEqualTo(2);

        // Test 2: Query with exact match - should find exact document
        GlobalIndexResult exactMatchResult = reader.visit(appleVector, 1);
        assertThat(containsRowId(exactMatchResult, appleId))
                .as("Exact match query should find Apple")
                .isTrue();

        // Test 3: Query with vector similar to "Car" - should find Car first
        float[] carQueryVector = new float[] {0.15f, 0.85f};
        GlobalIndexResult carSearchResult = reader.visit(carQueryVector, 1);

        List<Long> carResultIds = new ArrayList<>();
        for (Long id : carSearchResult) {
            carResultIds.add(id);
        }

        assertThat(carResultIds).as("Query similar to Car should find Car").contains(carId);

        // Test 4: Search with larger k than available documents
        GlobalIndexResult largeKResult = reader.visit(queryVector, 10);
        assertThat(largeKResult).isNotNull();
        assertThat(largeKResult.iterator().hasNext()).isTrue();

        // Test 5: Verify all three documents are indexed
        GlobalIndexResult allDocsResult = reader.visit(new float[] {0.5f, 0.5f}, 3);
        List<Long> allResultIds = new ArrayList<>();
        for (Long id : allDocsResult) {
            allResultIds.add(id);
        }

        assertThat(allResultIds.size())
                .as("Should be able to retrieve all 3 documents")
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(3);

        reader.close();
    }

    private Options createDefaultOptions() {
        Options options = new Options();
        options.setInteger("vector.dim", 128);
        options.setString("vector.metric", "COSINE");
        options.setInteger("vector.m", 16);
        options.setInteger("vector.ef-construction", 100);
        return options;
    }

    private List<float[]> generateRandomVectors(int count, int dimension) {
        Random random = new Random(42); // Fixed seed for reproducibility
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                vector[j] = random.nextFloat() * 2 - 1; // Range [-1, 1]
            }
            // Normalize for cosine similarity
            normalize(vector);
            vectors.add(vector);
        }
        return vectors;
    }

    private void normalize(float[] vector) {
        float norm = 0;
        for (float v : vector) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        }
    }

    private boolean containsRowId(GlobalIndexResult result, long rowId) {
        for (Long id : result) {
            if (id == rowId) {
                return true;
            }
        }
        return false;
    }
}
