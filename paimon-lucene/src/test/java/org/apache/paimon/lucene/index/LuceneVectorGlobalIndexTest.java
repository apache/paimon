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

package org.apache.paimon.lucene.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LuceneVectorGlobalIndexWriter} and {@link LuceneVectorGlobalIndexReader}. */
public class LuceneVectorGlobalIndexTest {

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
        return new GlobalIndexFileReader() {
            @Override
            public SeekableInputStream getInputStream(String fileName) throws IOException {
                return fileIO.newInputStream(new Path(path, fileName));
            }

            @Override
            public Path filePath(String fileName) {
                return new Path(path, fileName);
            }
        };
    }

    @Test
    public void testDifferentSimilarityFunctions() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"COSINE", "DOT_PRODUCT", "EUCLIDEAN"};

        for (String metric : metrics) {
            Options options = createDefaultOptions(dimension);
            options.setString("vector.metric", metric);
            LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            LuceneVectorGlobalIndexWriter writer =
                    new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(metricIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(metricIndexPath, result.fileName())),
                            result.meta()));

            try (LuceneVectorGlobalIndexReader reader =
                    new LuceneVectorGlobalIndexReader(fileReader, metas, vectorType)) {
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 3, fieldName);
                Optional<GlobalIndexResult> searchResult = reader.visitVectorSearch(vectorSearch);
                assertThat(searchResult).isPresent();
            }
        }
    }

    @Test
    public void testDifferentDimensions() throws IOException {
        int[] dimensions = {8, 32, 128, 256};

        for (int dimension : dimensions) {
            Options options = createDefaultOptions(dimension);
            LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            LuceneVectorGlobalIndexWriter writer =
                    new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

            int numVectors = 10;
            List<float[]> testVectors = generateRandomVectors(numVectors, dimension);
            testVectors.forEach(writer::write);

            List<ResultEntry> results = writer.finish();
            assertThat(results).hasSize(1);

            ResultEntry result = results.get(0);
            GlobalIndexFileReader fileReader = createFileReader(dimIndexPath);
            List<GlobalIndexIOMeta> metas = new ArrayList<>();
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(dimIndexPath, result.fileName())),
                            result.meta()));

            try (LuceneVectorGlobalIndexReader reader =
                    new LuceneVectorGlobalIndexReader(fileReader, metas, vectorType)) {
                // Verify search works with this dimension
                VectorSearch vectorSearch = new VectorSearch(testVectors.get(0), 5, fieldName);
                Optional<GlobalIndexResult> searchResult = reader.visitVectorSearch(vectorSearch);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDimensionMismatch() throws IOException {
        Options options = createDefaultOptions(64);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);

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
        LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (ResultEntry result : results) {
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.meta()));
        }

        try (LuceneVectorGlobalIndexReader reader =
                new LuceneVectorGlobalIndexReader(fileReader, metas, vectorType)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 1, fieldName);
            LuceneVectorSearchGlobalIndexResult result =
                    (LuceneVectorSearchGlobalIndexResult)
                            reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            long expectedRowId = 0;
            assertThat(containsRowId(result, expectedRowId)).isTrue();
            assertThat(result.scoreGetter().score(expectedRowId)).isEqualTo(1.0f);
            expectedRowId = 1;
            RoaringNavigableMap64 filterResults = new RoaringNavigableMap64();
            filterResults.add(expectedRowId);
            vectorSearch =
                    new VectorSearch(vectors[0], 1, fieldName).withIncludeRowIds(filterResults);
            result =
                    (LuceneVectorSearchGlobalIndexResult)
                            reader.visitVectorSearch(vectorSearch).get();
            assertThat(containsRowId(result, expectedRowId)).isTrue();

            float[] queryVector = new float[] {0.85f, 0.15f};
            vectorSearch = new VectorSearch(queryVector, 2, fieldName);
            result =
                    (LuceneVectorSearchGlobalIndexResult)
                            reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            long rowId1 = 1;
            long rowId2 = 3;
            assertThat(containsRowId(result, rowId1)).isTrue();
            assertThat(containsRowId(result, rowId2)).isTrue();
            assertThat(result.scoreGetter().score(rowId1)).isEqualTo(0.98765427f);
            assertThat(result.scoreGetter().score(rowId2)).isEqualTo(0.9738046f);
        }
    }

    @Test
    public void testByteVectorIndexEndToEnd() throws IOException {
        int dimension = 2;
        Options options = createDefaultOptions(dimension);
        int sizePerIndex = 3;
        options.setInteger("vector.size-per-index", sizePerIndex);

        byte[][] vectors =
                new byte[][] {
                    new byte[] {100, 0}, new byte[] {95, 10}, new byte[] {10, 95},
                    new byte[] {98, 5}, new byte[] {0, 100}, new byte[] {5, 98}
                };

        DataType byteVectorType = new ArrayType(new TinyIntType());
        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuceneVectorIndexOptions indexOptions = new LuceneVectorIndexOptions(options);
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, byteVectorType, indexOptions);
        Arrays.stream(vectors).forEach(writer::write);

        List<ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (ResultEntry result : results) {
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.meta()));
        }

        try (LuceneVectorGlobalIndexReader reader =
                new LuceneVectorGlobalIndexReader(fileReader, metas, byteVectorType)) {
            VectorSearch vectorSearch = new VectorSearch(vectors[0], 1, fieldName);
            GlobalIndexResult result = reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            assertThat(containsRowId(result, 0)).isTrue();

            byte[] queryVector = new byte[] {85, 15};
            vectorSearch = new VectorSearch(queryVector, 2, fieldName);
            result = reader.visitVectorSearch(vectorSearch).get();
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            assertThat(containsRowId(result, 1)).isTrue();
            assertThat(containsRowId(result, 3)).isTrue();
        }
    }

    @Test
    public void testInvalidTopK() {
        assertThatThrownBy(() -> new VectorSearch(new float[] {0.1f}, 0, fieldName))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Limit must be positive");
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger("vector.dim", dimension);
        String defaultMetric = "EUCLIDEAN";
        options.setString("vector.metric", defaultMetric);
        options.setInteger("vector.m", 16);
        options.setInteger("vector.ef-construction", 100);
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
            // Normalize for cosine similarity
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
