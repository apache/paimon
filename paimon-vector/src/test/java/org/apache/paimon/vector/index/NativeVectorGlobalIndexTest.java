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

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.IvfPqBatchTableReuseMode;
import org.apache.paimon.index.vector.VectorIndexReader;
import org.apache.paimon.index.vector.VectorSearchParams;
import org.apache.paimon.index.vector.VectorSearchResult;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
            Options options = new Options();
            options.setInteger("ivf-flat.dimension", 2);
            options.setString("ivf-flat.metric", "l2");
            options.setInteger("ivf-flat.nlist", 1);
            try (org.apache.paimon.index.vector.VectorIndexTrainer ignored =
                    org.apache.paimon.index.vector.VectorIndexTrainer.create(
                            NativeVectorGlobalIndexerFactory.nativeOptions(
                                    new ArrayType(new FloatType()),
                                    options,
                                    IvfFlatVectorGlobalIndexerFactory.IDENTIFIER,
                                    "vec"))) {
                // Closed immediately; constructing the trainer is enough to validate JNI loading.
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
    public void testMultiVectorArrayRequiresExplicitBuildDimension() {
        DataType type = new ArrayType(new ArrayType(new FloatType()));
        String kind = "ivf-flat";
        Map<String, String> options = Collections.singletonMap("metric", "cosine");
        assertThatThrownBy(
                        () ->
                                new NativeVectorGlobalIndexWriter(
                                        createFileWriter(indexPath), type, options, kind))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ivf-flat.dimension");
    }

    @Test
    public void testIncompleteMultiVectorRowCannotBePublished() {
        DataType type = new ArrayType(new ArrayType(new FloatType()));
        Options options = new Options();
        options.setInteger("ivf-flat.dimension", 2);
        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        type, options, "ivf-flat", fieldName);
        try (NativeVectorGlobalIndexWriter writer =
                new NativeVectorGlobalIndexWriter(
                        createFileWriter(indexPath), type, nativeOptions, "ivf-flat")) {
            GenericArray invalidRow =
                    new GenericArray(
                            new Object[] {
                                new GenericArray(new float[] {1f, 0f}),
                                new GenericArray(new float[] {1f})
                            });
            assertThatThrownBy(() -> writer.write(invalidRow, 7))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("dimension mismatch");
            assertThatThrownBy(writer::finish).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    public void testNativeMultiVectorFileStoresRowIds() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable());
        DataType type = new ArrayType(new VectorType(2, new FloatType()));
        Map<String, String> nativeOptions = new HashMap<>();
        nativeOptions.put("index.type", "ivf_flat");
        nativeOptions.put("metric", "cosine");
        nativeOptions.put("nlist", "1");
        float[] q = {1f, 0f};
        Object x = BinaryVector.fromPrimitiveArray(q);
        Object y = BinaryVector.fromPrimitiveArray(new float[] {0f, 1f});
        long rowId = (1L << 32) + 7;
        List<ResultEntry> files;
        try (NativeVectorGlobalIndexWriter writer =
                new NativeVectorGlobalIndexWriter(
                        createFileWriter(indexPath), type, nativeOptions, "ivf-flat")) {
            writer.write(new GenericArray(new Object[] {y, null, x, x}), rowId);
            writer.write(new GenericArray(new Object[] {y}), 19);
            writer.write(
                    new GenericArray(
                            new Object[] {BinaryVector.fromPrimitiveArray(new float[] {-1f, 0f})}),
                    42);
            files = writer.finish();
        }
        GlobalIndexIOMeta meta = toIOMetas(files, indexPath).get(0);
        try (SeekableInputStream in = createFileReader(indexPath).getInputStream(meta);
                VectorIndexReader reader =
                        new VectorIndexReader(
                                new NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput(
                                        in))) {
            assertThat(reader.metadata().totalVectors()).isEqualTo(5);
            assertThat(reader.search(q, VectorSearchParams.automatic(5)).ids())
                    .containsExactlyInAnyOrder(rowId, rowId, rowId, 19L, 42L);
        }
        try (GlobalIndexReader reader =
                new NativeVectorGlobalIndexer(type, nativeOptions, "ivf-flat")
                        .createReader(
                                createFileReader(indexPath),
                                Collections.singletonList(meta),
                                rowId + 1,
                                null,
                                executor)) {
            ScoredGlobalIndexResult hits =
                    reader.visitVectorSearch(new VectorSearch(q, 2, fieldName)).get().get();
            assertThat(hits.results().toArray(2)).containsExactly(19, rowId);
            assertThat(hits.scoreGetter().score(rowId)).isEqualTo(1f);
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            filter.add(rowId);
            ScoredGlobalIndexResult filtered =
                    reader.visitVectorSearch(
                                    new VectorSearch(q, 2, fieldName).withIncludeRowIds(filter))
                            .get()
                            .get();
            assertThat(filtered.results().toArray(2)).containsExactly(rowId);
            assertThat(filtered.scoreGetter().score(rowId)).isEqualTo(1f);
        }
    }

    @Test
    public void testEmptyMultiVectorRangeDoesNotCreateAFile() throws Exception {
        DataType type = new ArrayType(new VectorType(2, new FloatType()));
        String kind = "ivf-flat";
        Map<String, String> options =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        type, new Options(), kind, fieldName);
        try (NativeVectorGlobalIndexWriter writer =
                new NativeVectorGlobalIndexWriter(
                        createFileWriter(indexPath), type, options, kind)) {
            writer.write(null, 0);
            writer.write(new GenericArray(new Object[0]), 1);
            writer.write(new GenericArray(new Object[] {null}), 2);
            assertThat(writer.finish()).isEmpty();
            assertThat(fileIO.listStatus(indexPath)).isEmpty();
        }
    }

    @Test
    public void testMultiVectorCosineSearchAtExtremeScales() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable());
        DataType type = new ArrayType(new VectorType(2, new FloatType()));
        Options options = new Options();
        options.setInteger("ivf-flat.nlist", 1);
        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        type, options, "ivf-flat", fieldName);
        for (float storedScale : new float[] {1e-30f, 1e30f, 1f}) {
            List<ResultEntry> files;
            try (NativeVectorGlobalIndexWriter writer =
                    new NativeVectorGlobalIndexWriter(
                            createFileWriter(indexPath), type, nativeOptions, "ivf-flat")) {
                for (int i = 0; i < 8; i++) {
                    float x = i == 7 ? storedScale : -storedScale;
                    writer.write(
                            new GenericArray(
                                    new Object[] {
                                        BinaryVector.fromPrimitiveArray(new float[] {x, 0f})
                                    }),
                            i);
                }
                files = writer.finish();
            }
            try (GlobalIndexReader reader =
                    new NativeVectorGlobalIndexer(type, nativeOptions, "ivf-flat")
                            .createReader(
                                    createFileReader(indexPath),
                                    toIOMetas(files, indexPath),
                                    8,
                                    null,
                                    executor)) {
                for (float queryScale : new float[] {1e-30f, 1e30f, 1f}) {
                    float[] query = {queryScale, 0f};
                    ScoredGlobalIndexResult result =
                            reader.visitVectorSearch(new VectorSearch(query, 1, fieldName))
                                    .get()
                                    .get();
                    assertThat(result.results().toArray(1)).containsExactly(7);
                    assertThat(result.scoreGetter().score(7)).isEqualTo(1f);
                    assertThat(query).containsExactly(queryScale, 0f);
                }
            }
        }
    }

    @Test
    public void testMultiVectorSearchWidensDuplicateHeavyPages() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable());
        DataType type = new ArrayType(new VectorType(2, new FloatType()));
        Options options = new Options();
        options.setInteger("ivf-flat.nlist", 16);
        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(
                        type, options, "ivf-flat", fieldName);
        List<ResultEntry> files;
        try (NativeVectorGlobalIndexWriter writer =
                new NativeVectorGlobalIndexWriter(
                        createFileWriter(indexPath), type, nativeOptions, "ivf-flat")) {
            for (int row = 0; row < 64; row++) {
                double angle = row / 64.0;
                Object[] children = new Object[32];
                Arrays.fill(
                        children,
                        BinaryVector.fromPrimitiveArray(
                                new float[] {(float) Math.cos(angle), (float) Math.sin(angle)}));
                writer.write(new GenericArray(children), row);
            }
            files = writer.finish();
        }

        List<Integer> widths = new ArrayList<>();
        try (NativeVectorGlobalIndexReader reader =
                new NativeVectorGlobalIndexReader(
                        createFileReader(indexPath),
                        toIOMetas(files, indexPath),
                        type,
                        executor,
                        "ivf-flat",
                        64) {
                    @Override
                    VectorSearchResult searchVector(
                            float[] vector, VectorSearchParams params, byte[] filter) {
                        widths.add(params.topK());
                        return super.searchVector(vector, params, filter);
                    }
                }) {
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            long[] expected = new long[16];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = 4 * i;
                filter.add(expected[i]);
            }
            ScoredGlobalIndexResult result =
                    reader.visitVectorSearch(
                                    new VectorSearch(
                                                    new float[] {1, 0},
                                                    expected.length,
                                                    fieldName,
                                                    Collections.singletonMap(
                                                            "ivf.max_initial_filter_expansion_factor",
                                                            "1"))
                                            .withIncludeRowIds(filter))
                            .get()
                            .get();
            assertThat(result.results().toArray(expected.length)).containsExactly(expected);
            assertThat(widths.get(0)).isEqualTo(expected.length);
            assertThat(widths.size()).isLessThanOrEqualTo(8);
        }
    }

    @Test
    public void testMultiVectorParentFilter() throws Exception {
        Assumptions.assumeTrue(isNativeAvailable());
        DataType type = new ArrayType(new VectorType(2, new FloatType()));
        String kind = "ivf-flat";
        Options options = new Options();
        options.setInteger("ivf-flat.nlist", 1);
        Map<String, String> nativeOptions =
                NativeVectorGlobalIndexerFactory.nativeOptions(type, options, kind, fieldName);
        assertThat(nativeOptions.get("dimension")).isEqualTo("2");
        float[] q = {1f, 0f};
        try (NativeVectorGlobalIndexWriter writer =
                new NativeVectorGlobalIndexWriter(
                        createFileWriter(indexPath), type, nativeOptions, kind)) {
            writer.write(null, 7);
            writer.write(new GenericArray(new Object[] {null}), 19);
            writer.write(new GenericArray(new Object[0]), 0);
            Object x = BinaryVector.fromPrimitiveArray(q);
            Object y = BinaryVector.fromPrimitiveArray(new float[] {0f, 1f});
            Object opposite = BinaryVector.fromPrimitiveArray(new float[] {-1f, 0f});
            writer.write(new GenericArray(new Object[] {x, x, x, null}), 23);
            writer.write(new GenericArray(new Object[] {y, y, y}), 5);
            writer.write(new GenericArray(new Object[] {opposite}), 11);
            List<ResultEntry> files = writer.finish();
            assertThat(files).hasSize(1);
            assertThat(files.get(0).rowCount()).isEqualTo(6);
            try (GlobalIndexReader reader =
                    new NativeVectorGlobalIndexer(type, nativeOptions, kind)
                            .createReader(
                                    createFileReader(indexPath),
                                    toIOMetas(files, indexPath),
                                    24,
                                    null,
                                    executor)) {
                Optional<ScoredGlobalIndexResult> hits =
                        reader.visitVectorSearch(new VectorSearch(q, 3, fieldName)).get();
                assertThat(hits.get().results().toArray(3)).containsExactly(5, 11, 23);
                RoaringNavigableMap64 filter = new RoaringNavigableMap64();
                filter.add(5);
                ScoredGlobalIndexResult filtered =
                        reader.visitVectorSearch(
                                        new VectorSearch(q, 1, fieldName).withIncludeRowIds(filter))
                                .get()
                                .get();
                assertThat(filtered.results().toArray(1)).containsExactly(5);
                assertThat(filtered.scoreGetter().score(5))
                        .isCloseTo(0f, org.assertj.core.data.Offset.offset(0.00001f));
                filter.add(23);
                filter.add(19); // Null list inside the range.
                AtomicInteger encodings = new AtomicInteger();
                RoaringNavigableMap64 batchFilter =
                        new RoaringNavigableMap64() {
                            @Override
                            public byte[] serialize() throws IOException {
                                encodings.incrementAndGet();
                                return super.serialize();
                            }
                        };
                batchFilter.or(filter);
                List<Optional<ScoredGlobalIndexResult>> batch =
                        reader.visitBatchVectorSearch(
                                        new BatchVectorSearch(
                                                        new float[][] {q, {0f, 1f}}, 3, fieldName)
                                                .withIncludeRowIds(batchFilter))
                                .get();
                assertThat(batch).hasSize(2);
                for (Optional<ScoredGlobalIndexResult> result : batch) {
                    assertThat(result.get().results().toArray(3)).containsExactly(5, 23);
                }
                assertThat(encodings.get()).isEqualTo(1);
                assertThat(batch.get(0).get().scoreGetter().score(23)).isEqualTo(1f);
                assertThat(batch.get(1).get().scoreGetter().score(5)).isEqualTo(1f);
                assertThat(batchFilter.toArray(3)).containsExactly(5, 19, 23);
            }
        }
    }

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
    public void testTrainingVectorCountUsesConfiguredSampleRatio() {
        assertThat(NativeVectorGlobalIndexWriter.trainingCandidateCount(10000000000L, 0.6))
                .isEqualTo(6000000000L);
        assertThat(
                        NativeVectorGlobalIndexWriter.sampleIndex(
                                5999999999L, 10000000000L, 6000000000L))
                .isEqualTo(9999999998L);
        assertThat(NativeVectorGlobalIndexWriter.trainingVectorCount(10_000L, 1.0))
                .isEqualTo(10_000);
        assertThat(NativeVectorGlobalIndexWriter.trainingVectorCount(10_000L, 0.25))
                .isEqualTo(2_500);
        assertThat(NativeVectorGlobalIndexWriter.trainingVectorCount(3L, 0.01)).isEqualTo(1);
        assertThat(NativeVectorGlobalIndexWriter.trainingVectorCount(0L, 0.25)).isEqualTo(0);

        assertThatThrownBy(() -> NativeVectorGlobalIndexWriter.trainingVectorCount(10L, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than 0");
        assertThatThrownBy(() -> NativeVectorGlobalIndexWriter.trainingVectorCount(10L, 1.1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("less than or equal to 1");
    }

    @Test
    public void testSampleIndexSequenceIsUniform() {
        assertThat(
                        new long[] {
                            NativeVectorGlobalIndexWriter.sampleIndex(0, 10, 4),
                            NativeVectorGlobalIndexWriter.sampleIndex(1, 10, 4),
                            NativeVectorGlobalIndexWriter.sampleIndex(2, 10, 4),
                            NativeVectorGlobalIndexWriter.sampleIndex(3, 10, 4)
                        })
                .containsExactly(0L, 2L, 5L, 7L);
    }

    @Test
    public void testVectorBatchSizeProtectsSingleJavaArrayAllocation() {
        assertThat(NativeVectorGlobalIndexWriter.vectorBatchSize(4096, 128)).isEqualTo(4096);
        assertThat(NativeVectorGlobalIndexWriter.vectorBatchSize(10000, 128)).isEqualTo(10000);
        assertThat(
                        NativeVectorGlobalIndexWriter.vectorBatchSize(
                                4096, NativeVectorGlobalIndexWriter.MAX_FLOAT_ARRAY_LENGTH))
                .isEqualTo(1);
        assertThat(
                        NativeVectorGlobalIndexWriter.vectorBatchSize(
                                10000, NativeVectorGlobalIndexWriter.MAX_FLOAT_ARRAY_LENGTH))
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
        parameters.put("ignored", "bad");

        VectorSearchParams ivfParams = NativeVectorGlobalIndexReader.searchParams(parameters, 10);
        assertThat(ivfParams.topK()).isEqualTo(10);

        VectorSearchParams automaticParams =
                NativeVectorGlobalIndexReader.searchParams(Collections.emptyMap(), 5);
        assertThat(automaticParams.topK()).isEqualTo(5);

        VectorSearchParams diskAnnParams =
                NativeVectorGlobalIndexReader.searchParams(
                        Collections.singletonMap("diskann.l_search", "80"), 10);
        assertThat(diskAnnParams.topK()).isEqualTo(10);
    }

    @Test
    public void testIvfInitialFilterExpansionFactorValidationIsPropagated() {
        assertThatThrownBy(
                        () ->
                                NativeVectorGlobalIndexReader.searchParams(
                                        Collections.singletonMap(
                                                "ivf.max_initial_filter_expansion_factor", "0"),
                                        10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("greater than 0");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("ivf.nprobe", "16");
        parameters.put("ivf.max_initial_filter_expansion_factor", "4");
        assertThatThrownBy(() -> NativeVectorGlobalIndexReader.searchParams(parameters, 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires automatic IVF search");
    }

    @Test
    public void testIvfPqBatchTableReuseIsPropagatedToBatchSearchParams() {
        VectorSearchParams params =
                NativeVectorGlobalIndexReader.batchSearchParams(
                        Collections.singletonMap("ivf_pq.batch_table_reuse", "on"), 10);

        assertThat(params.ivfPqBatchTableReuse()).isEqualTo(IvfPqBatchTableReuseMode.ON);
    }

    @Test
    public void testIvfPqBatchTableReuseMaxBytesIsPropagated() {
        VectorSearchParams params =
                NativeVectorGlobalIndexReader.batchSearchParams(
                        Collections.singletonMap("ivf_pq.batch_table_reuse.max_bytes", "134217728"),
                        10);

        assertThat(params.ivfPqBatchTableReuseMaxBytes()).isEqualTo(128L * 1024 * 1024);
    }

    @Test
    public void testIvfPqBatchTableReuseMaxBytesSupportsLongValues() {
        VectorSearchParams params =
                NativeVectorGlobalIndexReader.batchSearchParams(
                        Collections.singletonMap(
                                "ivf_pq.batch_table_reuse.max_bytes", "5368709120"),
                        10);

        assertThat(params.ivfPqBatchTableReuseMaxBytes()).isEqualTo(5L * 1024 * 1024 * 1024);
    }

    @Test
    public void testVectorSearchParameterRangeValidationDelegatedToNative() {
        assertThat(
                        NativeVectorGlobalIndexReader.searchParams(
                                Collections.singletonMap("ivf.nprobe", "0"), 10))
                .isNotNull();
        assertThat(
                        NativeVectorGlobalIndexReader.searchParams(
                                Collections.singletonMap("diskann.l_search", "-1"), 10))
                .isNotNull();

        Map<String, String> conflicting = new HashMap<>();
        conflicting.put("ivf.nprobe", "16");
        conflicting.put("diskann.l_search", "100");
        assertThatThrownBy(() -> NativeVectorGlobalIndexReader.searchParams(conflicting, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot set both");
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
    public void testNewIndexTypesEndToEnd() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        float[][] vectors = new float[32][2];
        for (int i = 0; i < vectors.length; i++) {
            double angle = 2 * Math.PI * i / vectors.length;
            vectors[i][0] = (float) Math.cos(angle);
            vectors[i][1] = (float) Math.sin(angle);
        }

        for (String identifier :
                new String[] {
                    IvfSqVectorGlobalIndexerFactory.IDENTIFIER,
                    IvfRqVectorGlobalIndexerFactory.IDENTIFIER,
                    DiskAnnVectorGlobalIndexerFactory.IDENTIFIER
                }) {
            Options options = new Options();
            options.setInteger(identifier + ".dimension", 2);
            options.setString(identifier + ".metric", "l2");
            if (IvfRqVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier)) {
                options.setInteger(identifier + ".nlist", 1);
            } else {
                if (DiskAnnVectorGlobalIndexerFactory.IDENTIFIER.equals(identifier)) {
                    options.setInteger("diskann.max-degree", 4);
                    options.setInteger("diskann.build-search-list-size", 8);
                }
            }

            NativeVectorGlobalIndexWriter writer =
                    new NativeVectorGlobalIndexWriter(
                            createFileWriter(indexPath),
                            vectorType,
                            NativeVectorGlobalIndexerFactory.nativeOptions(
                                    vectorType, options, identifier, fieldName),
                            identifier);
            writeVectors(writer, vectors);
            List<GlobalIndexIOMeta> metas = toIOMetas(writer.finish(), indexPath);

            try (NativeVectorGlobalIndexReader reader =
                    new NativeVectorGlobalIndexReader(
                            createFileReader(indexPath), metas, vectorType, executor)) {
                ScoredGlobalIndexResult result =
                        reader.visitVectorSearch(new VectorSearch(vectors[0], 2, fieldName))
                                .join()
                                .get();
                assertThat(result.results().contains(0L)).as(identifier).isTrue();
            }
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
                (NativeVectorGlobalIndexReader)
                        indexer.createReader(fileReader, metas, vectors.length, null, executor)) {
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
