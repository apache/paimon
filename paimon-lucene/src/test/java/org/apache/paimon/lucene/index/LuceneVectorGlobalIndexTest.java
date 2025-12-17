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
import org.apache.paimon.types.TinyIntType;

import org.junit.jupiter.api.AfterEach;
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

/** Test for {@link LuceneVectorGlobalIndexWriter} and {@link LuceneVectorGlobalIndexReader}. */
public class LuceneVectorGlobalIndexTest {

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
    public void testDifferentSimilarityFunctions() throws IOException {
        int dimension = 32;
        int numVectors = 20;

        String[] metrics = {"COSINE", "DOT_PRODUCT", "EUCLIDEAN"};

        for (String metric : metrics) {
            Options options = createDefaultOptions(dimension);
            options.setString("vector.metric", metric);

            Path metricIndexPath = new Path(indexPath, metric.toLowerCase());
            GlobalIndexFileWriter fileWriter = createFileWriter(metricIndexPath);
            LuceneVectorGlobalIndexWriter writer =
                    new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, options);

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

            try (LuceneVectorGlobalIndexReader reader =
                    new LuceneVectorGlobalIndexReader(fileReader, metas)) {
                GlobalIndexResult searchResult = reader.search(testVectors.get(0), 3);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDifferentDimensions() throws IOException {
        int[] dimensions = {8, 32, 128, 256};

        for (int dimension : dimensions) {
            Options options = createDefaultOptions(dimension);

            Path dimIndexPath = new Path(indexPath, "dim_" + dimension);
            GlobalIndexFileWriter fileWriter = createFileWriter(dimIndexPath);
            LuceneVectorGlobalIndexWriter writer =
                    new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, options);

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

            try (LuceneVectorGlobalIndexReader reader =
                    new LuceneVectorGlobalIndexReader(fileReader, metas)) {
                // Verify search works with this dimension
                GlobalIndexResult searchResult = reader.search(testVectors.get(0), 5);
                assertThat(searchResult).isNotNull();
            }
        }
    }

    @Test
    public void testDimensionMismatch() throws IOException {
        Options options = createDefaultOptions(64);

        GlobalIndexFileWriter fileWriter = createFileWriter(indexPath);
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, options);

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
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, vectorType, options);
        Arrays.stream(vectors).forEach(writer::write);

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            GlobalIndexWriter.ResultEntry result = results.get(i);
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));
        }

        try (LuceneVectorGlobalIndexReader reader =
                new LuceneVectorGlobalIndexReader(fileReader, metas)) {
            GlobalIndexResult result = reader.search(vectors[0], 1);
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            assertThat(containsRowId(result, 1)).isTrue();

            float[] queryVector = new float[] {0.85f, 0.15f};
            result = reader.search(queryVector, 2);
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            assertThat(containsRowId(result, 2)).isTrue();
            assertThat(containsRowId(result, 4)).isTrue();
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
        LuceneVectorGlobalIndexWriter writer =
                new LuceneVectorGlobalIndexWriter(fileWriter, byteVectorType, options);
        Arrays.stream(vectors).forEach(writer::write);

        List<GlobalIndexWriter.ResultEntry> results = writer.finish();
        assertThat(results).hasSize(2);

        GlobalIndexFileReader fileReader = createFileReader(indexPath);
        List<GlobalIndexIOMeta> metas = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            GlobalIndexWriter.ResultEntry result = results.get(i);
            metas.add(
                    new GlobalIndexIOMeta(
                            result.fileName(),
                            fileIO.getFileSize(new Path(indexPath, result.fileName())),
                            result.rowRange().to - result.rowRange().from,
                            result.meta()));
        }

        try (LuceneVectorGlobalIndexReader reader =
                new LuceneVectorGlobalIndexReader(fileReader, metas)) {
            GlobalIndexResult result = reader.search(vectors[0], 1);
            assertThat(result.results().getLongCardinality()).isEqualTo(1);
            assertThat(containsRowId(result, 1)).isTrue();

            byte[] queryVector = new byte[] {85, 15};
            result = reader.search(queryVector, 2);
            assertThat(result.results().getLongCardinality()).isEqualTo(2);
            assertThat(containsRowId(result, 2)).isTrue();
            assertThat(containsRowId(result, 4)).isTrue();
        }
    }

    private Options createDefaultOptions(int dimension) {
        Options options = new Options();
        options.setInteger("vector.dim", dimension);
        options.setString("vector.metric", "EUCLIDEAN");
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
