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

package org.apache.paimon.elasticsearch.index;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.elasticsearch.index.model.ESIndexEntryMeta;
import org.apache.paimon.elasticsearch.index.model.ESScoredGlobalIndexResult;
import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ESVectorGlobalIndexWriter} and {@link ESVectorGlobalIndexReader}. */
public class ESVectorGlobalIndexTest {

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private Path indexPath;
    private DataType vectorType;
    private final String fieldName = "vec";

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
        assertThat(results).hasSize(2);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (ResultEntry result : results) {
            Path filePath = new Path(path, result.fileName());
            metas.add(new GlobalIndexIOMeta(filePath, fileIO.getFileSize(filePath), result.meta()));
        }
        return metas;
    }

    @Test
    public void testFloatVectorIndexEndToEnd() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);

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
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            // Query vector[0] = (1.0, 0.0); nearest neighbors by L2 should be
            // row 0 (1.0, 0.0), row 3 (0.98, 0.05), row 1 (0.95, 0.1).
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(3L)).isTrue();
            float scoreRow0 = result.scoreGetter().score(0L);
            float scoreRow3 = result.scoreGetter().score(3L);
            assertThat(scoreRow0).isGreaterThanOrEqualTo(scoreRow3);

            // Test with multiple results
            float[] queryVector = new float[] {0.85f, 0.15f};
            vectorSearch = new VectorSearch(queryVector, 2, fieldName);
            result = (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
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
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {

            // Unfiltered: query (1,0) top-3 should come from the first cluster (rows 0,1,2).
            VectorSearch search = new VectorSearch(vectors[0], 3, fieldName);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(1L)).isTrue();
            assertThat(result.results().contains(2L)).isTrue();

            // Filter to row 3 only.
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            filter.add(3L);
            search = new VectorSearch(vectors[0], 3, fieldName).withIncludeRowIds(filter);
            result = (ESScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(3L)).isTrue();

            // Filter spanning multiple rows: {1, 4}.
            RoaringNavigableMap64 crossFilter = new RoaringNavigableMap64();
            crossFilter.add(1L);
            crossFilter.add(4L);
            search = new VectorSearch(vectors[0], 6, fieldName).withIncludeRowIds(crossFilter);
            result = (ESScoredGlobalIndexResult) reader.visitVectorSearch(search).get();
            assertThat(result.results().contains(1L)).isTrue();
            assertThat(result.results().contains(4L)).isTrue();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
        }
    }

    @Test
    public void testDifferentMetrics() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"l2", "cosine", "inner_product"};

        for (String metric : metrics) {
            Options options = createDefaultOptions(dimension);
            options.setString("es.distance.metric", metric);
            ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            List<GlobalIndexIOMeta> metas = toIOMetas(results, metricIndexPath);

            GlobalIndexFileReader fileReader = createFileReader(metricIndexPath);
            try (ESVectorGlobalIndexReader reader =
                    new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 3, fieldName);
                ESScoredGlobalIndexResult searchResult =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
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
            ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

            int numVectors = 10;
            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            List<GlobalIndexIOMeta> metas = toIOMetas(results, dimIndexPath);

            GlobalIndexFileReader fileReader = createFileReader(dimIndexPath);
            try (ESVectorGlobalIndexReader reader =
                    new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 5, fieldName);
                ESScoredGlobalIndexResult searchResult =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(5);
                assertThat(searchResult.results().contains(0L)).isTrue();
                float score = searchResult.scoreGetter().score(0L);
                assertThat(score).isNotNaN();
            }
        }
    }

    @Test
    public void testDimensionMismatch() throws IOException {
        Options options = createDefaultOptions(64);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

        float[] wrongDimVector = new float[32];
        assertThatThrownBy(() -> writer.write(wrongDimVector))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dimension mismatch");
    }

    @Test
    public void testLargeVectorSet() throws IOException {
        int dimension = 32;
        Options options = createDefaultOptions(dimension);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

        int numVectors = 350;
        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        testVectors.forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        Path filePath = new Path(indexPath, results.get(0).fileName());
        assertThat(fileIO.exists(filePath)).isTrue();
        assertThat(fileIO.getFileSize(filePath)).isGreaterThan(0);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            for (int queryIdx : new int[] {50, 150, 320}) {
                VectorSearch vectorSearch =
                        new VectorSearch(testVectors.get(queryIdx), 3, fieldName);
                ESScoredGlobalIndexResult searchResult =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(3);
                assertThat(searchResult.results().contains((long) queryIdx)).isTrue();
                assertThat(searchResult.scoreGetter().score((long) queryIdx)).isNotNaN();
            }

            VectorSearch vectorSearch = new VectorSearch(testVectors.get(200), 5, fieldName);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(5);
            assertThat(result.results().contains(200L)).isTrue();
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
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vecFieldType, indexOptions);

        for (float[] vec : vectors) {
            writer.write(BinaryVector.fromPrimitiveArray(vec));
        }

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, vecIndexPath);

        GlobalIndexFileReader fileReader = createFileReader(vecIndexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vecFieldType, indexOptions)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 3, fieldName);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(3);
            assertThat(result.results().contains(0L)).isTrue();
            assertThat(result.results().contains(3L)).isTrue();
        }
    }

    @Test
    public void testVectorTypeRejectsNonFloatElement() {
        DataType intVecType = new VectorType(2, new IntType());
        Options options = createDefaultOptions(2);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);

        assertThatThrownBy(() -> createWriter(fileWriter, intVecType, indexOptions))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("float");
    }

    @Test
    public void testBug001DiagnosticBytes() throws IOException {
        int dimension = 4;
        int numVectors = 20;
        Options options = createDefaultOptions(dimension);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);
        for (int id = 0; id < numVectors; id++) {
            writer.write(new float[] {id * 0.1f, 0.5f, 0.5f, 0.5f});
        }
        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        // Find archive meta by deserializing ESIndexEntryMeta
        GlobalIndexIOMeta archiveMeta = null;
        GlobalIndexIOMeta offsetsMeta = null;
        for (GlobalIndexIOMeta m : metas) {
            ESIndexEntryMeta entryMeta = ESIndexEntryMeta.deserialize(m.metadata());
            if (entryMeta.fileRole() == ESIndexEntryMeta.FILE_ROLE_ARCHIVE) {
                archiveMeta = m;
            } else {
                offsetsMeta = m;
            }
        }
        assertThat(archiveMeta).isNotNull();
        assertThat(offsetsMeta).isNotNull();

        // Parse offset table to find .mivf range
        byte[] tableBytes;
        try (org.apache.paimon.fs.SeekableInputStream in =
                fileIO.newInputStream(offsetsMeta.filePath())) {
            java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) {
                bos.write(buf, 0, n);
            }
            tableBytes = bos.toByteArray();
        }
        java.io.DataInputStream dis =
                new java.io.DataInputStream(new java.io.ByteArrayInputStream(tableBytes));
        int version = dis.readInt();
        int fileCount = dis.readInt();
        java.util.Map<String, long[]> offsets = new java.util.LinkedHashMap<>();
        for (int i = 0; i < fileCount; i++) {
            String name = dis.readUTF();
            long offset = dis.readLong();
            long length = dis.readLong();
            offsets.put(name, new long[] {offset, length});
        }

        System.out.println("=== BUG-001 Diagnostic ===");
        System.out.println("Archive file: " + archiveMeta.filePath());
        System.out.println("Offset table entries: " + offsets.size());
        for (java.util.Map.Entry<String, long[]> e : offsets.entrySet()) {
            System.out.printf(
                    "  %s: offset=%d, length=%d%n", e.getKey(), e.getValue()[0], e.getValue()[1]);
        }

        // Read first 16 bytes of .mivf from archive
        String mivfName = null;
        for (String name : offsets.keySet()) {
            if (name.endsWith(".mivf")) {
                mivfName = name;
            }
        }
        assertThat(mivfName).isNotNull();
        long[] mivfRange = offsets.get(mivfName);

        byte[] mivfHead = new byte[16];
        try (org.apache.paimon.fs.SeekableInputStream in =
                fileIO.newInputStream(archiveMeta.filePath())) {
            in.seek(mivfRange[0]);
            in.read(mivfHead);
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : mivfHead) {
            sb.append(String.format("%02x ", b));
        }
        System.out.println(".mivf first 16 bytes at offset " + mivfRange[0] + ": " + sb);
        System.out.printf(
                "Big-endian int: 0x%08x%n",
                ((mivfHead[0] & 0xFF) << 24)
                        | ((mivfHead[1] & 0xFF) << 16)
                        | ((mivfHead[2] & 0xFF) << 8)
                        | (mivfHead[3] & 0xFF));
        System.out.printf(
                "Little-endian int: 0x%08x%n",
                ((mivfHead[3] & 0xFF) << 24)
                        | ((mivfHead[2] & 0xFF) << 16)
                        | ((mivfHead[1] & 0xFF) << 8)
                        | (mivfHead[0] & 0xFF));
        System.out.printf("CODEC_MAGIC = 0x3fd76c17%n");

        // Also read raw .mivf file from temp dir to compare
        // (the archive should contain identical bytes)
    }

    @Test
    public void testBug001ReproSmall() throws IOException {
        int dimension = 4;
        int numVectors = 100;
        Options options = createDefaultOptions(dimension);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

        for (int id = 0; id < numVectors; id++) {
            float[] vec = new float[] {id * 0.01f, (id % 5) * 0.1f, 0.5f, 0.5f};
            writer.write(vec);
        }

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            float[] queryVector = new float[] {0.5f, 0.2f, 0.5f, 0.5f};
            VectorSearch vectorSearch = new VectorSearch(queryVector, 5, fieldName);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result).isNotNull();
            assertThat(result.results().getLongCardinality()).isEqualTo(5);
        }
    }

    @Test
    public void testBug001ReproWithFilter() throws IOException {
        int dimension = 4;
        int numVectors = 100;
        Options options = createDefaultOptions(dimension);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

        for (int id = 0; id < numVectors; id++) {
            float[] vec = new float[] {id * 0.01f, (id % 5) * 0.1f, 0.5f, 0.5f};
            writer.write(vec);
        }

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            // category=0 的行: 0, 5, 10, ..., 95 (共 20 行)
            RoaringNavigableMap64 filter = new RoaringNavigableMap64();
            for (int id = 0; id < numVectors; id++) {
                if (id % 5 == 0) {
                    filter.add((long) id);
                }
            }
            float[] queryVector = new float[] {0.5f, 0.0f, 0.5f, 0.5f};
            VectorSearch vectorSearch =
                    new VectorSearch(queryVector, 5, fieldName).withIncludeRowIds(filter);
            ESScoredGlobalIndexResult result =
                    (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
            assertThat(result).isNotNull();
            assertThat(result.results().getLongCardinality()).isEqualTo(5);
            for (long rowId : result.results()) {
                assertThat(rowId % 5).isEqualTo(0);
            }
        }
    }

    @Test
    public void testInvalidTopK() {
        assertThatThrownBy(() -> new VectorSearch(new float[] {0.1f}, 0, fieldName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");
    }

    private ESVectorGlobalIndexWriter createWriter(
            GlobalIndexFileWriter fileWriter, DataType fieldType, ESVectorIndexOptions options)
            throws IOException {
        return new ESVectorGlobalIndexWriter(fileWriter, fieldName, fieldType, options);
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger(ESVectorIndexOptions.DIMENSION.key(), dimension);
        options.setString(ESVectorIndexOptions.DISTANCE_METRIC.key(), "l2");
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

    @Test
    public void testParallelSearch() throws IOException {
        int dimension = 32;
        Options options = createDefaultOptions(dimension);
        options.setString("es.search.parallel", "true");

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        ESVectorIndexOptions indexOptions = new ESVectorIndexOptions(options);
        ESVectorGlobalIndexWriter writer = createWriter(fileWriter, vectorType, indexOptions);

        int numVectors = 350;
        List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
        testVectors.forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        List<GlobalIndexIOMeta> metas = toIOMetas(results, indexPath);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        try (ESVectorGlobalIndexReader reader =
                new ESVectorGlobalIndexReader(fileReader, metas, vectorType, indexOptions)) {
            for (int queryIdx : new int[] {50, 150, 320}) {
                VectorSearch vectorSearch =
                        new VectorSearch(testVectors.get(queryIdx), 3, fieldName);
                ESScoredGlobalIndexResult searchResult =
                        (ESScoredGlobalIndexResult) reader.visitVectorSearch(vectorSearch).get();
                assertThat(searchResult.results().getLongCardinality()).isEqualTo(3);
                assertThat(searchResult.results().contains((long) queryIdx)).isTrue();
                assertThat(searchResult.scoreGetter().score((long) queryIdx)).isNotNaN();
            }
        }
    }
}
