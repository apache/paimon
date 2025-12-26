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

package org.apache.paimon.faiss.index;

import org.apache.paimon.faiss.jni.FaissJNI;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link FaissVectorGlobalIndexWriter} and {@link FaissVectorGlobalIndexReader}. */
public class FaissVectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;
    private final String fieldName = "vec";

    @BeforeEach
    public void setup() {
        // Skip tests if FAISS native library is not available
        Assumptions.assumeTrue(
                FaissJNI.isLoaded(), "FAISS native library not available, skipping test");

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
    public void testDifferentMetrics() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"L2", "INNER_PRODUCT"};

        for (String metric : metrics) {
            Options options = createDefaultOptions(dimension);
            options.setString("vector.metric", metric);
            FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            FaissVectorGlobalIndexWriter writer =
                    new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(metricIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(metricIndexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));

            try (FaissVectorGlobalIndexReader reader =
                    new FaissVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 3, fieldName);
                GlobalIndexResult searchResult = reader.visitVectorSearch(vectorSearch);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDifferentIndexTypes() throws IOException {
        int dimension = 32;
        int numVectors = 100; // More vectors for IVF training

        String[] indexTypes = {"FLAT", "HNSW"};
        // IVF and IVF_PQ require more training data, tested separately

        for (String indexType : indexTypes) {
            Options options = createDefaultOptions(dimension);
            options.setString("vector.index-type", indexType);
            FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
            Path typeIndexPath = new Path(indexPath, indexType.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(typeIndexPath);
            FaissVectorGlobalIndexWriter writer =
                    new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(typeIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(typeIndexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));

            try (FaissVectorGlobalIndexReader reader =
                    new FaissVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 5, fieldName);
                GlobalIndexResult searchResult = reader.visitVectorSearch(vectorSearch);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDifferentDimensions() throws IOException {
        int[] dimensions = {8, 32, 128, 256};

        for (int dimension : dimensions) {
            Options options = createDefaultOptions(dimension);
            FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            FaissVectorGlobalIndexWriter writer =
                    new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            int numVectors = 10;
            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<GlobalIndexWriter.ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            GlobalIndexWriter.ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(dimIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(dimIndexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));

            try (FaissVectorGlobalIndexReader reader =
                    new FaissVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 5, fieldName);
                GlobalIndexResult searchResult = reader.visitVectorSearch(vectorSearch);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDimensionMismatch() throws IOException {
        Options options = createDefaultOptions(64);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
        FaissVectorGlobalIndexWriter writer =
                new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

        // Try to write vector with wrong dimension
        float[] wrongDimVector = new float[32]; // Wrong dimension
        assertThatThrownBy(() -> writer.write(wrongDimVector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testFloatVectorIndexEndToEnd() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        int sizePerIndex = 3;
        options.setInteger("vector.size-per-index", sizePerIndex);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f}, new float[] {0.95f, 0.1f}, new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f}, new float[] {0.0f, 1.0f}, new float[] {0.05f, 0.98f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
        FaissVectorGlobalIndexWriter writer =
                new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (GlobalIndexWriter.ResultEntry result : results) {
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));
        }

        try (FaissVectorGlobalIndexReader reader =
                new FaissVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 1, fieldName);
            FaissVectorSearchGlobalIndexResult result =
                    (FaissVectorSearchGlobalIndexResult) reader.visitVectorSearch(vectorSearch);
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            long expectedRowId = 0;
            assertThat(containsRowId(result, expectedRowId)).isTrue();

            // Test with filter
            expectedRowId = 1;
            RoaringNavigableMap64 filterResults = new RoaringNavigableMap64();
            filterResults.add(expectedRowId);
            vectorSearch =
                    new VectorSearch(vectors[0], 1, fieldName).withIncludeRowIds(filterResults);
            result = (FaissVectorSearchGlobalIndexResult) reader.visitVectorSearch(vectorSearch);
            assertThat(containsRowId(result, expectedRowId)).isTrue();

            // Test with multiple results
            float[] queryVector = new float[] {0.85f, 0.15f};
            vectorSearch = new VectorSearch(queryVector, 2, fieldName);
            result = (FaissVectorSearchGlobalIndexResult) reader.visitVectorSearch(vectorSearch);
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
        }
    }

    @Test
    public void testInvalidTopK() {
        assertThatThrownBy(() -> new VectorSearch(new float[] {0.1f}, 0, fieldName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");
    }

    @Test
    public void testMultipleIndexFiles() throws IOException {
        int dimension = 32;
        Options options = createDefaultOptions(dimension);
        options.setInteger("vector.size-per-index", 5); // Small batch size

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        FaissVectorIndexOptions indexOptions = new FaissVectorIndexOptions(options);
        FaissVectorGlobalIndexWriter writer =
                new FaissVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

        int numVectors = 15; // Will create 3 index files
        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        testVectors.forEach(writer::write);

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(3);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (GlobalIndexWriter.ResultEntry result : results) {
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));
        }

        try (FaissVectorGlobalIndexReader reader =
                new FaissVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            // Search should work across all index files
            VectorSearch vectorSearch = new VectorSearch(testVectors.get(10), 3, fieldName);
            GlobalIndexResult searchResult = reader.visitVectorSearch(vectorSearch);
            assertThat(searchResult).isNotNull();
            assertThat(searchResult.results().getLongCardinality()).isGreaterThan(0);
        }
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger("vector.dim", dimension);
        options.setString("vector.metric", "L2");
        options.setString("vector.index-type", "HNSW");
        options.setInteger("vector.m", 32);
        options.setInteger("vector.ef-construction", 40);
        options.setInteger("vector.ef-search", 16);
        return options;
    }

    private List<float[]> generateRandomVectors(int count, int dimension) {
        Random random = new Random(42);
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                vector[j] = random.nextFloat() * 2 - 1; // Range [-1, 1]
            }
            // Normalize for inner product similarity
            float norm = 0;
            for (float v : vector) {
                norm += v * v;
            }
            norm = (float) Math.sqrt(norm);
            if (norm > 0) {
                for (int m = 0; m < vector.length; m++) {
                    vector[m] /= norm;
                }
            }
            vectors.add(vector);
        }
        return vectors;
    }

    private boolean containsRowId(GlobalIndexResult result, long rowId) {
        List<Long> resultIds = new ArrayList<>();
        result.results().iterator().forEachRemaining(resultIds::add);
        return resultIds.contains(rowId);
    }
}
