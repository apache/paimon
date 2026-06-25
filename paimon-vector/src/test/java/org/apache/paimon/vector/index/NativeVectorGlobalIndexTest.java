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

package org.apache.paimon.vector.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VectorType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeVectorGlobalIndexWriter} and {@link NativeVectorGlobalIndexReader}. */
public class NativeVectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private static final String IVF_PQ_IDENTIFIER =
            IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER;
    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;
    private final String fieldName = "vec";
    private ExecutorService executor;

    private static boolean isNativeAvailable() {
        try {
            NativeVectorIndexLoader.loadJni();
            Options options = new Options();
            options.setInteger("ivf-flat.dimension", 2);
            options.setString("ivf-flat.metric", "l2");
            options.setInteger("ivf-flat.nlist", 1);
            try (org.apache.paimon.index.vector.VectorIndexWriter ignored =
                    new org.apache.paimon.index.vector.VectorIndexWriter(
                            NativeVectorGlobalIndexerFactory.nativeOptions(
                                    new ArrayType(new FloatType()),
                                    options,
                                    IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                    "vec"))) {
                // Closed immediately; constructing the writer is enough to validate JNI loading.
            }
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @BeforeEach
    public void setup() {
        fileIO = new LocalFileIO();
        indexPath = new Path(tempDir.toString());
        vectorType = new ArrayType(new FloatType());
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void cleanup() throws IOException {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (fileIO != null) {
            fileIO.delete(indexPath, true);
        }
    }

    // =================== Tests that do NOT need native library =====================

    @Test
    public void testDimensionMismatch() {
        Options options = createDefaultOptions(64);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        float[] wrongDimVector = new float[32];
        assertThatThrownBy(() -> writer.write(wrongDimVector, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testVectorTypeRejectsNonFloatElement() {
        DataType intVecType = new VectorType(2, new IntType());
        Options options = createDefaultOptions(2);
        options.setInteger("ivf-pq.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);

        assertThatThrownBy(() -> createIvfPqWriter(fileWriter, intVecType, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("float");
    }

    @Test
    public void testNanInVectorRejected() {
        Options options = createDefaultOptions(2);
        options.setInteger("ivf-pq.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        assertThatThrownBy(() -> writer.write(new float[] {1.0f, Float.NaN}, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowId=0")
                .hasMessageContaining("index=1")
                .hasMessageContaining("NaN");
    }

    @Test
    public void testInfinityInVectorRejected() {
        Options options = createDefaultOptions(2);
        options.setInteger("ivf-pq.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(null, 0); // row 0 - null
        assertThatThrownBy(() -> writer.write(new float[] {Float.POSITIVE_INFINITY, 0.0f}, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowId=1")
                .hasMessageContaining("index=0")
                .hasMessageContaining("Infinity");
    }

    @Test
    public void testAllNullReturnsEmpty() {
        Options options = createDefaultOptions(2);
        options.setInteger("ivf-pq.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(null, 0);
        writer.write(null, 1);
        writer.write(null, 2);

        List<ResultEntry> results = writer.finish();
        assertThat(results).isEmpty();
    }

    @Test
    public void testTrainingVectorCountUsesOnlyConfiguredSampleLimit() {
        int oldJavaArrayLimitFor1024Dim =
                NativeVectorGlobalIndexWriter.MAX_FLOAT_ARRAY_LENGTH / 1024;
        int requestedSamples = oldJavaArrayLimitFor1024Dim + 1;

        assertThat(
                        NativeVectorGlobalIndexWriter.trainingVectorCount(
                                requestedSamples, requestedSamples))
                .isEqualTo(requestedSamples);
        assertThat(NativeVectorGlobalIndexWriter.trainingVectorCount(10_000L, 64)).isEqualTo(64);
    }

    @Test
    public void testTrainingBatchSizeProtectsSingleJavaArrayAllocation() {
        assertThat(NativeVectorGlobalIndexWriter.vectorBatchSize(4096, 128)).isEqualTo(4096);
        assertThat(
                        NativeVectorGlobalIndexWriter.vectorBatchSize(
                                4096, NativeVectorGlobalIndexWriter.MAX_FLOAT_ARRAY_LENGTH))
                .isEqualTo(1);
        assertThatThrownBy(() -> NativeVectorGlobalIndexWriter.vectorBatchSize(4096, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive integer");
    }

    @Test
    public void testMetaSerializationIsEmptyMap() throws IOException {
        VectorIndexMeta meta = new VectorIndexMeta();
        byte[] serialized = meta.serialize();
        VectorIndexMeta deserialized = VectorIndexMeta.deserialize(serialized);

        assertThat(new String(serialized, StandardCharsets.UTF_8)).isEqualTo("{}");
        assertThat(new String(deserialized.serialize(), StandardCharsets.UTF_8)).isEqualTo("{}");
    }

    @Test
    public void testVectorSearchParameterParsing() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("ivf.nprobe", "24");
        parameters.put("hnsw.ef_search", "80");
        parameters.put("ignored", "bad");

        assertThat(NativeVectorGlobalIndexReader.nprobe(parameters)).isEqualTo(24);
        assertThat(NativeVectorGlobalIndexReader.efSearch(parameters)).isEqualTo(80);
        assertThat(NativeVectorGlobalIndexReader.nprobe(Collections.emptyMap())).isEqualTo(16);
        assertThat(NativeVectorGlobalIndexReader.efSearch(Collections.emptyMap())).isEqualTo(0);
    }

    @Test
    public void testVectorSearchParameterRangeValidationDelegatedToNative() {
        assertThat(
                        NativeVectorGlobalIndexReader.nprobe(
                                Collections.singletonMap("ivf.nprobe", "0")))
                .isEqualTo(0);
        assertThat(
                        NativeVectorGlobalIndexReader.efSearch(
                                Collections.singletonMap("hnsw.ef_search", "-1")))
                .isEqualTo(-1);
    }

    @Test
    public void testInnerProductDistanceConvertedToHigherIsBetterScore() {
        ScoredGlobalIndexResult result =
                NativeVectorGlobalIndexReader.buildScoredResult(
                                new long[] {10L, 20L, 30L},
                                new float[] {-1.0f, -0.5f, -0.1f},
                                "inner_product")
                        .get();

        assertThat(result.scoreGetter().score(10L)).isEqualTo(1.0f);
        assertThat(result.scoreGetter().score(20L)).isEqualTo(0.5f);
        assertThat(result.scoreGetter().score(30L)).isEqualTo(0.1f);

        ScoredGlobalIndexResult top1 = result.topK(1);
        assertThat(top1.results().getLongCardinality()).isEqualTo(1);
        assertThat(top1.results().contains(10L)).isTrue();
    }

    // =================== Tests that NEED native library =====================

    @Test
    public void testFloatVectorEndToEnd() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.05f, 0.98f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            ScoredGlobalIndexResult result = reader.visitVectorSearch(vectorSearch).join().get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            float score = result.scoreGetter().score(0L);
            assertThat(score).isNotNaN();
        }
    }

    @Test
    public void testSearchWithRoaringFilter() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

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
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {

            // Filter to rows {1, 4} only
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            filter.add(1L);
            filter.add(4L);
            VectorSearch search =
                    new VectorSearch(vectors[0], 6, fieldName).withIncludeRowIds(filter);
            ScoredGlobalIndexResult result = reader.visitVectorSearch(search).join().get();
            assertThat(result.results().contains(1L)).isTrue();
            assertThat(result.results().contains(4L)).isTrue();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
        }
    }

    @Test
    public void testNullVectorSkipWithCorrectIds() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.0f, 1.0f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(vectors[0], 0); // row 0
        writer.write(null, 1); // row 1 - null
        writer.write(vectors[1], 2); // row 2
        writer.write(null, 3); // row 3 - null
        writer.write(null, 4); // row 4 - null
        writer.write(vectors[2], 5); // row 5

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowCount()).isEqualTo(6);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            ScoredGlobalIndexResult result = reader.visitVectorSearch(vectorSearch).join().get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(2L)).isTrue();
            assertThat(result.results().contains(5L)).isTrue();
            assertThat(result.results().contains(1L)).isFalse();
            assertThat(result.results().contains(3L)).isFalse();
            assertThat(result.results().contains(4L)).isFalse();
        }
    }

    @Test
    public void testViaIndexer() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.7f, 0.7f}
                };

        NativeVectorGlobalIndexer indexer =
                new NativeVectorGlobalIndexer(
                        vectorType,
                        NativeVectorGlobalIndexerFactory.nativeOptions(
                                vectorType, options, IVF_PQ_IDENTIFIER, fieldName),
                        IVF_PQ_IDENTIFIER);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer =
                (NativeVectorGlobalIndexWriter) indexer.createWriter(fileWriter);
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
        }
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                (NativeVectorGlobalIndexReader) indexer.createReader(fileReader, metas, executor)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 2, fieldName);
            ScoredGlobalIndexResult result = reader.visitVectorSearch(vectorSearch).join().get();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            assertThat(result.results().contains(0L)).isTrue();
        }
    }

    @Test
    public void testBatchVectorSearch() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.05f, 0.98f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        writeVectors(writer, vectors);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
            float[][] queryVectors =
                    new float[][] {
                        new float[] {1.0f, 0.0f},
                        new float[] {0.0f, 1.0f},
                        new float[] {0.7f, 0.7f}
                    };
            BatchVectorSearch batchSearch = new BatchVectorSearch(queryVectors, 3, fieldName);
            List<Optional<ScoredGlobalIndexResult>> batchResults =
                    reader.visitBatchVectorSearch(batchSearch).join();

            // result i corresponds to queryVectors[i], in input order.
            assertThat(batchResults).hasSize(3);

            assertThat(batchResults.get(0)).isPresent();
            assertThat(batchResults.get(0).get().results().contains(0L)).isTrue();

            assertThat(batchResults.get(1)).isPresent();
            assertThat(batchResults.get(1).get().results().contains(4L)).isTrue();

            assertThat(batchResults.get(2)).isPresent();
            assertThat(batchResults.get(2).get().results().getLongCardinality()).isEqualTo(3);
        }
    }

    @Test
    public void testBatchVectorSearchWithFilter() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

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
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        writeVectors(writer, vectors);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
            float[][] queryVectors =
                    new float[][] {new float[] {1.0f, 0.0f}, new float[] {-1.0f, 0.0f}};

            // Both queries are scoped to rows {1, 4}.
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            filter.add(1L);
            filter.add(4L);

            BatchVectorSearch batchSearch =
                    new BatchVectorSearch(queryVectors, 6, fieldName).withIncludeRowIds(filter);
            List<Optional<ScoredGlobalIndexResult>> batchResults =
                    reader.visitBatchVectorSearch(batchSearch).join();

            assertThat(batchResults).hasSize(2);

            assertThat(batchResults.get(0)).isPresent();
            assertThat(batchResults.get(0).get().results().getLongCardinality()).isEqualTo(2);
            assertThat(batchResults.get(0).get().results().contains(1L)).isTrue();
            assertThat(batchResults.get(0).get().results().contains(4L)).isTrue();

            assertThat(batchResults.get(1)).isPresent();
            assertThat(batchResults.get(1).get().results().getLongCardinality()).isEqualTo(2);
            assertThat(batchResults.get(1).get().results().contains(1L)).isTrue();
            assertThat(batchResults.get(1).get().results().contains(4L)).isTrue();
        }
    }

    @Test
    public void testBatchConsistentWithSingle() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("ivf-pq.nlist", 2);
        options.setInteger("ivf-pq.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.95f, 0.1f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.98f, 0.05f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.05f, 0.98f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        NativeVectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        writeVectors(writer, vectors);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
            float[][] queryVectors =
                    new float[][] {
                        new float[] {1.0f, 0.0f},
                        new float[] {0.0f, 1.0f},
                        new float[] {0.7f, 0.7f}
                    };
            int limit = 3;

            // The batch path must return exactly what looping the single path returns, in order.
            BatchVectorSearch batchSearch = new BatchVectorSearch(queryVectors, limit, fieldName);
            List<Optional<ScoredGlobalIndexResult>> batchResults =
                    reader.visitBatchVectorSearch(batchSearch).join();

            assertThat(batchResults).hasSize(queryVectors.length);
            for (int i = 0; i < queryVectors.length; i++) {
                VectorSearch singleSearch = new VectorSearch(queryVectors[i], limit, fieldName);
                Optional<ScoredGlobalIndexResult> singleResult =
                        reader.visitVectorSearch(singleSearch).join();

                assertThat(batchResults.get(i).isPresent()).isEqualTo(singleResult.isPresent());
                if (singleResult.isPresent()) {
                    assertThat(batchResults.get(i).get().results().getLongCardinality())
                            .isEqualTo(singleResult.get().results().getLongCardinality());
                    for (long rowId : singleResult.get().results()) {
                        assertThat(batchResults.get(i).get().results().contains(rowId)).isTrue();
                    }
                }
            }
        }
    }

    // =================== Helpers =====================

    private NativeVectorGlobalIndexWriter createIvfPqWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        return new NativeVectorGlobalIndexWriter(
                fileWriter,
                fieldType,
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        fieldType, options, IVF_PQ_IDENTIFIER, fieldName),
                IVF_PQ_IDENTIFIER);
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger("ivf-pq.dimension", dimension);
        options.setString("ivf-pq.metric", "l2");
        return options;
    }

    private void writeVectors(NativeVectorGlobalIndexWriter writer, float[][] vectors) {
        for (int i = 0; i < vectors.length; i++) {
            writer.write(vectors[i], i);
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
}
