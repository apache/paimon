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
import org.apache.paimon.index.ivfpq.IndexType;
import org.apache.paimon.index.ivfpq.NativeLoader;
import org.apache.paimon.options.Options;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link VectorGlobalIndexWriter} and {@link VectorGlobalIndexReader}. */
public class VectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private static final String IVF_PQ_IDENTIFIER =
            IvfPqAlgorithmVectorGlobalIndexerFactory.IDENTIFIER;
    private static final String IVF_HNSW_FLAT_IDENTIFIER =
            IvfHnswFlatVectorGlobalIndexerFactory.IDENTIFIER;

    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;
    private final String fieldName = "vec";
    private ExecutorService executor;

    private static boolean isNativeAvailable() {
        try {
            NativeLoader.loadJni();
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
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        float[] wrongDimVector = new float[32];
        assertThatThrownBy(() -> writer.write(wrongDimVector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testVectorTypeRejectsNonFloatElement() {
        DataType intVecType = new VectorType(2, new IntType());
        Options options = createDefaultOptions(2);
        options.setInteger("vector.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);

        assertThatThrownBy(() -> createIvfPqWriter(fileWriter, intVecType, options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("float");
    }

    @Test
    public void testNanInVectorRejected() {
        Options options = createDefaultOptions(2);
        options.setInteger("vector.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        assertThatThrownBy(() -> writer.write(new float[] {1.0f, Float.NaN}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowId=0")
                .hasMessageContaining("index=1")
                .hasMessageContaining("NaN");
    }

    @Test
    public void testInfinityInVectorRejected() {
        Options options = createDefaultOptions(2);
        options.setInteger("vector.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(null); // row 0 - null, advances logicalRowId
        assertThatThrownBy(() -> writer.write(new float[] {Float.POSITIVE_INFINITY, 0.0f}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowId=1")
                .hasMessageContaining("index=0")
                .hasMessageContaining("Infinity");
    }

    @Test
    public void testAllNullReturnsEmpty() {
        Options options = createDefaultOptions(2);
        options.setInteger("vector.pq.m", 1);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(null);
        writer.write(null);
        writer.write(null);

        List<ResultEntry> results = writer.finish();
        assertThat(results).isEmpty();
    }

    @Test
    public void testMetaSerializationRoundTrip() throws IOException {
        Options options = new Options();
        options.setInteger("vector.index.dimension", 32);
        options.setString("vector.distance.metric", "cosine");
        options.setInteger("vector.nlist", 64);
        options.setInteger("vector.pq.m", 8);
        options.setString("vector.pq.use-opq", "true");
        options.setInteger("vector.nprobe", 24);

        VectorIndexMeta meta = new VectorIndexMeta(metaOptions(IVF_PQ_IDENTIFIER, options));
        byte[] serialized = meta.serialize();
        VectorIndexMeta deserialized = VectorIndexMeta.deserialize(serialized);

        assertThat(deserialized.dimension()).isEqualTo(32);
        assertThat(deserialized.indexType()).isEqualTo(IndexType.IVF_PQ);
        assertThat(deserialized.metric()).isEqualTo("cosine");
        assertThat(deserialized.nlist()).isEqualTo(64);
        assertThat(deserialized.m()).isEqualTo(8);
        assertThat(deserialized.useOpq()).isTrue();
        assertThat(deserialized.nprobe()).isEqualTo(24);
    }

    @Test
    public void testMetaSerializationRoundTripForHnsw() throws IOException {
        Options options = new Options();
        options.setInteger("vector.index.dimension", 16);
        options.setString("vector.distance.metric", "l2");
        options.setInteger("vector.nlist", 8);
        options.setInteger("vector.hnsw.m", 12);
        options.setInteger("vector.hnsw.ef-construction", 64);
        options.setInteger("vector.hnsw.max-level", 5);
        options.setInteger("vector.hnsw.ef-search", 80);

        VectorIndexMeta deserialized =
                VectorIndexMeta.deserialize(
                        new VectorIndexMeta(metaOptions(IVF_HNSW_FLAT_IDENTIFIER, options))
                                .serialize());

        assertThat(deserialized.indexType()).isEqualTo(IndexType.IVF_HNSW_FLAT);
        assertThat(deserialized.dimension()).isEqualTo(16);
        assertThat(deserialized.hnswM()).isEqualTo(12);
        assertThat(deserialized.hnswEfConstruction()).isEqualTo(64);
        assertThat(deserialized.hnswMaxLevel()).isEqualTo(5);
        assertThat(deserialized.efSearch()).isEqualTo(80);
    }

    // =================== Tests that NEED native library =====================

    @Test
    public void testFloatVectorEndToEnd() throws IOException {
        Assumptions.assumeTrue(isNativeAvailable(), "Vector index native library not available");

        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        options.setInteger("vector.nlist", 2);
        options.setInteger("vector.pq.m", 1);

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
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (VectorGlobalIndexReader reader =
                new VectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
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
        options.setInteger("vector.nlist", 2);
        options.setInteger("vector.pq.m", 1);

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
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (VectorGlobalIndexReader reader =
                new VectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {

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
        options.setInteger("vector.nlist", 2);
        options.setInteger("vector.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.1f, 0.95f},
                    new float[] {0.0f, 1.0f}
                };

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer = createIvfPqWriter(fileWriter, vectorType, options);

        writer.write(vectors[0]); // row 0
        writer.write(null); // row 1 - null
        writer.write(vectors[1]); // row 2
        writer.write(null); // row 3 - null
        writer.write(null); // row 4 - null
        writer.write(vectors[2]); // row 5

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(1);
        assertThat(results.get(0).rowCount()).isEqualTo(6);

        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);
        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (VectorGlobalIndexReader reader =
                new VectorGlobalIndexReader(fileReader, metas, vectorType, executor)) {
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
        options.setInteger("vector.nlist", 2);
        options.setInteger("vector.pq.m", 1);

        float[][] vectors =
                new float[][] {
                    new float[] {1.0f, 0.0f},
                    new float[] {0.0f, 1.0f},
                    new float[] {0.7f, 0.7f}
                };

        VectorGlobalIndexer indexer =
                new VectorGlobalIndexer(vectorType, options, IndexType.IVF_PQ, IVF_PQ_IDENTIFIER);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        VectorGlobalIndexWriter writer = (VectorGlobalIndexWriter) indexer.createWriter(fileWriter);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (VectorGlobalIndexReader reader =
                (VectorGlobalIndexReader) indexer.createReader(fileReader, metas, executor)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 2, fieldName);
            ScoredGlobalIndexResult result = reader.visitVectorSearch(vectorSearch).join().get();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            assertThat(result.results().contains(0L)).isTrue();
        }
    }

    // =================== Helpers =====================

    private VectorGlobalIndexWriter createIvfPqWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, Options options) {
        return new VectorGlobalIndexWriter(
                fileWriter, fieldType, options, IndexType.IVF_PQ, IVF_PQ_IDENTIFIER);
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger("vector.index.dimension", dimension);
        options.setString("vector.distance.metric", "l2");
        return options;
    }

    private Map<String, String> metaOptions(String indexType, Options options) {
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put(VectorIndexMeta.KEY_INDEX_TYPE, indexType);
        meta.put(
                VectorIndexMeta.KEY_DIMENSION,
                String.valueOf(options.getInteger("vector.index.dimension", 128)));
        meta.put(
                VectorIndexMeta.KEY_METRIC,
                options.getString("vector.distance.metric", "inner_product"));
        meta.put(
                VectorIndexMeta.KEY_NLIST, String.valueOf(options.getInteger("vector.nlist", 256)));
        meta.put(VectorIndexMeta.KEY_M, String.valueOf(options.getInteger("vector.pq.m", 16)));
        meta.put(
                VectorIndexMeta.KEY_USE_OPQ,
                String.valueOf(options.getBoolean("vector.pq.use-opq", false)));
        meta.put(
                VectorIndexMeta.KEY_HNSW_M,
                String.valueOf(options.getInteger("vector.hnsw.m", 20)));
        meta.put(
                VectorIndexMeta.KEY_HNSW_EF_CONSTRUCTION,
                String.valueOf(options.getInteger("vector.hnsw.ef-construction", 150)));
        meta.put(
                VectorIndexMeta.KEY_HNSW_MAX_LEVEL,
                String.valueOf(options.getInteger("vector.hnsw.max-level", 7)));
        meta.put(
                VectorIndexMeta.KEY_NPROBE,
                String.valueOf(options.getInteger("vector.nprobe", 16)));
        meta.put(
                VectorIndexMeta.KEY_EF_SEARCH,
                String.valueOf(options.getInteger("vector.hnsw.ef-search", 0)));
        return meta;
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
