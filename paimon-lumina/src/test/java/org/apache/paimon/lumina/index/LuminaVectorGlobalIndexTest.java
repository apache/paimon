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

package org.apache.paimon.lumina.index;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.aliyun.lumina.Lumina;
import org.aliyun.lumina.LuminaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LuminaVectorGlobalIndexWriter} and {@link LuminaVectorGlobalIndexReader}. */
public class LuminaVectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;
    private final String fieldName = "vec";

    @BeforeEach
    public void setup() {
        if (!Lumina.isLibraryLoaded()) {
            try {
                Lumina.loadLibrary();
            } catch (LuminaException e) {
                StringBuilder errorMsg = new StringBuilder("Lumina native library not available.");
                errorMsg.append("\nError: ").append(e.getMessage());
                if (e.getCause() != null) {
                    errorMsg.append("\nCause: ").append(e.getCause().getMessage());
                }
                errorMsg.append(
                        "\n\nTo run Lumina tests, ensure the paimon-lumina-jni JAR"
                                + " with native libraries is available in the classpath.");
                Assumptions.assumeTrue(false, errorMsg.toString());
            }
        }

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
            public PositionOutputStream newOutputStream(String fileName) throws IOException {
                return fileIO.newOutputStream(new Path(path, fileName), false);
            }
        };
    }

    private GlobalIndexFileReader createFileReader(Path path) {
        return meta -> fileIO.newInputStream(new Path(path, meta.filePath()));
    }

    private List<GlobalIndexIOMeta> toIOMetas(List<ResultEntry> results, Path path)
            throws IOException {
        assertThat(results).hasSize(1);
        ResultEntry result = results.get(0);
        Path filePath = new Path(path, result.fileName());
        return Collections.singletonList(
                new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), result.meta()));
    }

    @Test
    public void testDifferentMetrics() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"l2", "cosine", "inner_product"};

        for (String metric : metrics) {
            Options options = createDefaultOptions(dimension);
            options.setString("lumina.distance.metric", metric);
            if ("cosine".equals(metric)) {
                // Lumina v0.1.0 does not support PQ + cosine combination
                options.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), "rawf32");
            }
            LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            LuminaVectorGlobalIndexWriter writer =
                    new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            List<GlobalIndexIOMeta> metas = toIOMetas(results, metricIndexPath);

            GlobalIndexFileReader fileReader = createFileReader(metricIndexPath);
            try (LuminaVectorGlobalIndexReader reader =
                    new LuminaVectorGlobalIndexReader(
                            fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 3, fieldName);
                LuminaScoredGlobalIndexResult searchResult =
                        (LuminaScoredGlobalIndexResult)
                                reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(3);
                assertThat(searchResult.results().contains(0L)).isTrue();
                float score = searchResult.scoreGetter().score(0L);
                assertThat(score).isNotNaN();
            }
        }
    }

    @Test
    public void testDifferentDimensions() throws IOException {
        int[] dimensions = {8, 32, 128, 256};

        for (int dimension : dimensions) {
            Options options = createDefaultOptions(dimension);
            LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            LuminaVectorGlobalIndexWriter writer =
                    new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            int numVectors = 10;
            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            List<GlobalIndexIOMeta> metas = toIOMetas(results, dimIndexPath);

            GlobalIndexFileReader fileReader = createFileReader(dimIndexPath);
            try (LuminaVectorGlobalIndexReader reader =
                    new LuminaVectorGlobalIndexReader(
                            fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 5, fieldName);
                LuminaScoredGlobalIndexResult searchResult =
                        (LuminaScoredGlobalIndexResult)
                                reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(5);
                assertThat(searchResult.results().contains(0L)).isTrue();
                float score = searchResult.scoreGetter().score(0L);
                assertThat(score).isNotNaN();
            }
        }
    }

    @Test
    public void testDimensionMismatch() {
        Options options = createDefaultOptions(64);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

        float[] wrongDimVector = new float[32];
        assertThatThrownBy(() -> writer.write(wrongDimVector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testFloatVectorIndexEndToEnd() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f}, new float[] {0.95f, 0.1f}, new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f}, new float[] {0.0f, 1.0f}, new float[] {0.05f, 0.98f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            // Query vector[0] = (1.0, 0.0); nearest neighbors by L2 should be
            // row 0 (1.0, 0.0), row 3 (0.98, 0.05), row 1 (0.95, 0.1).
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            LuminaScoredGlobalIndexResult result =
                    (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(3L)).isTrue();
            float scoreRow0 = result.scoreGetter().score(0L);
            float scoreRow3 = result.scoreGetter().score(3L);
            assertThat(scoreRow0).isGreaterThanOrEqualTo(scoreRow3);

            // Test with filter: only row 1
            long expectedRowId = 1;
            RoaringNavigableMap64 filterResults = new RoaringNavigableMap64();
            filterResults.add(expectedRowId);
            vectorSearch =
                    new VectorSearch(vectors[0], 3, fieldName).withIncludeRowIds(filterResults);
            result = (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            assertThat(result.results().contains(expectedRowId)).isTrue();

            // Test with multiple results
            float[] queryVector = new float[] {0.85f, 0.15f};
            vectorSearch = new VectorSearch(queryVector, 2, fieldName);
            result = (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
        }
    }

    @Test
    public void testSearchWithFilter() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.9f, 0.2f},
                    new float[] {-1.0f, 0.0f},
                    new float[] {-0.95f, 0.1f},
                    new float[] {-0.9f, 0.2f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {

            // Unfiltered: query (1,0) top-3 should come from the first cluster (rows 0,1,2).
            VectorSearch search = new VectorSearch(vectors[0], 3, fieldName);
            LuminaScoredGlobalIndexResult result =
                    (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(1L)).isTrue();
            assertThat(result.results().contains(2L)).isTrue();

            // Filter to row 3 only.
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            filter.add(3L);
            search = new VectorSearch(vectors[0], 3, fieldName).withIncludeRowIds(filter);
            result = (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(3L)).isTrue();
            assertThat(result.results().getLongCardinality()).isEqualTo(1);

            // Filter spanning multiple rows: {1, 4}.
            RoaringNavigableMap64 crossFilter = new RoaringNavigableMap64();
            crossFilter.add(1L);
            crossFilter.add(4L);
            search = new VectorSearch(vectors[0], 6, fieldName).withIncludeRowIds(crossFilter);
            result = (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(1L)).isTrue();
            assertThat(result.results().contains(4L)).isTrue();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
        }
    }

    @Test
    public void testPQWithCosineRejected() {
        Options options = new Options();
        options.setInteger(LuminaVectorIndexOptions.DIMENSION.key(), 32);
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "cosine");
        options.setString(LuminaVectorIndexOptions.ENCODING_TYPE.key(), "pq");
        assertThatThrownBy(() -> new LuminaVectorIndexOptions(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PQ encoding with cosine metric");
    }

    @Test
    public void testInvalidTopK() {
        assertThatThrownBy(() -> new VectorSearch(new float[] {0.1f}, 0, fieldName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");
    }

    @Test
    public void testLargeVectorSet() throws IOException {
        int dimension = 32;
        Options options = createDefaultOptions(dimension);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

        int numVectors = 350;
        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        testVectors.forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        Path filePath = new Path(indexPath, results.get(0).fileName());
        assertThat(fileIO.exists(filePath)).isTrue();
        assertThat(fileIO.getFileSize(filePath)).isGreaterThan(0);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            for (int queryIdx : new int[] {50, 150, 320}) {
                VectorSearch vectorSearch =
                        new VectorSearch(testVectors.get(queryIdx), 3, fieldName);
                LuminaScoredGlobalIndexResult searchResult =
                        (LuminaScoredGlobalIndexResult)
                                reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(3);
                assertThat(searchResult.results().contains((long) queryIdx)).isTrue();
                assertThat(searchResult.scoreGetter().score((long) queryIdx)).isNotNaN();
            }

            VectorSearch vectorSearch = new VectorSearch(testVectors.get(200), 5, fieldName);
            LuminaScoredGlobalIndexResult result =
                    (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(5);
            assertThat(result.results().contains(200L)).isTrue();
        }
    }

    @Test
    public void testReaderMetaOptionsOverrideDefaultOptions() throws IOException {
        // Write index with dimension=2
        int dimension = 2;
        Options writeOptions = createDefaultOptions(dimension);
        LuminaVectorIndexOptions writeIndexOptions = new LuminaVectorIndexOptions(writeOptions);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.0f, 1.0f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vectorType, writeIndexOptions);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        // Read with default options (dimension=128, the default) — simulates
        // the case where table options do not contain lumina.index.dimension.
        // The reader should still work because meta options written at build time
        // override the stale default dimension.
        Options readOptions = new Options();
        readOptions.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        // Do NOT set dimension — it defaults to 128
        LuminaVectorIndexOptions readIndexOptions = new LuminaVectorIndexOptions(readOptions);
        assertThat(readIndexOptions.dimension()).isEqualTo(128);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(
                        fileReader, metas, vectorType, readIndexOptions)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            LuminaScoredGlobalIndexResult result =
                    (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
        }
    }

    @Test
    public void testVectorTypeEndToEnd() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        DataType vecFieldType = new VectorType(dimension, new FloatType());

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.05f, 0.98f}
                };

        Path vecIndexPath = new Path(indexPath, "vector_type");
        GlobalIndexFileWriter fileWriter = createFileWriter(vecIndexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vecFieldType, indexOptions);

        // Write using BinaryVector (InternalVector)
        for (float[] vec : vectors) {
            writer.write(BinaryVector.fromPrimitiveArray(vec));
        }

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, vecIndexPath);

        GlobalIndexFileReader fileReader = createFileReader(vecIndexPath);
        try (LuminaVectorGlobalIndexReader reader =
                new LuminaVectorGlobalIndexReader(fileReader, metas, vecFieldType, indexOptions)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            LuminaScoredGlobalIndexResult result =
                    (LuminaScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(3L)).isTrue();
        }
    }

    @Test
    public void testVectorTypeWithFloatArrayWrite() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        DataType vecFieldType = new VectorType(dimension, new FloatType());

        Path vecIndexPath = new Path(indexPath, "vector_type_float");
        GlobalIndexFileWriter fileWriter = createFileWriter(vecIndexPath);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        LuminaVectorGlobalIndexWriter writer =
                new LuminaVectorGlobalIndexWriter(fileWriter, vecFieldType, indexOptions);

        // Write using raw float[] with VectorType field type
        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.7f, 0.7f}
                };
        for (float[] vec : vectors) {
            writer.write(vec);
        }

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
    }

    @Test
    public void testVectorTypeRejectsNonFloatElement() {
        DataType intVecType = new VectorType(2, new IntType());
        Options options = createDefaultOptions(2);
        LuminaVectorIndexOptions indexOptions = new LuminaVectorIndexOptions(options);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);

        assertThatThrownBy(
                        () ->
                                new LuminaVectorGlobalIndexWriter(
                                        fileWriter, intVecType, indexOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("float vector");
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger(LuminaVectorIndexOptions.DIMENSION.key(), dimension);
        options.setString(LuminaVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
        return options;
    }

    private List<float[]> generateRandomVectors(int count, int dimension) {
        Random random = new Random(42);
        List<float[]> vectors = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            float[] vector = new float[dimension];
            for (int j = 0; j < dimension; j++) {
                vector[j] = random.nextFloat() * 2 - 1;
            }
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
}
