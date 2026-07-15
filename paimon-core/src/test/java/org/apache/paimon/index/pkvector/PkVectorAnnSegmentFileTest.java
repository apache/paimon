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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Range;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests ANN construction from generic vector readers. */
class PkVectorAnnSegmentFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testBuildSkipsNullAndExcludedPhysicalRows() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        IndexFileMeta segment =
                annFile(fileIO)
                        .build(
                                Collections.singletonList(
                                        new PkVectorAnnSegmentFile.Source(
                                                dataFile("data-1", 3),
                                                new ArrayReader(
                                                        new float[][] {{0, 0}, null, {2, 0}}),
                                                position -> position == 0)),
                                vectorField(),
                                indexOptions(),
                                "l2",
                                "test-vector-ann");

        assertThat(segment.indexType()).isEqualTo("test-vector-ann");
        assertThat(segment.rowCount()).isEqualTo(1);
        PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(segment);
        assertThat(sourceMeta.sourceFiles())
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-1");
    }

    @Test
    void testBuildsSearchableEmptySegmentWhenAllRowsAreExcluded() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = annFile(fileIO);
        IndexFileMeta segment =
                annFile.build(
                        Collections.singletonList(
                                new PkVectorAnnSegmentFile.Source(
                                        dataFile("data-1", 2),
                                        new ArrayReader(new float[][] {{0, 0}, {1, 0}}),
                                        position -> true)),
                        vectorField(),
                        indexOptions(),
                        "l2",
                        "test-vector-ann");

        assertThat(segment.rowCount()).isZero();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThat(
                            new PkVectorAnnSegmentSearcher(
                                            fileIO,
                                            annFile,
                                            vectorField(),
                                            indexOptions(),
                                            "l2",
                                            executor)
                                    .search(
                                            segment,
                                            PrimaryKeyIndexSourceMeta.fromIndexFile(segment),
                                            new float[] {0, 0},
                                            1,
                                            Collections.emptyMap(),
                                            Collections.emptyMap()))
                    .isEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testBuildsAndSearchesMultiSourceSegment() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = annFile(fileIO);
        IndexFileMeta segment =
                annFile.build(
                        Arrays.asList(
                                new PkVectorAnnSegmentFile.Source(
                                        dataFile("data-1", 2),
                                        new ArrayReader(new float[][] {{5, 0}, {10, 0}})),
                                new PkVectorAnnSegmentFile.Source(
                                        dataFile("data-2", 2),
                                        new ArrayReader(new float[][] {{0, 0}, {2, 0}}))),
                        vectorField(),
                        indexOptions(),
                        "l2",
                        "test-vector-ann");
        assertThat(segment.globalIndexMeta().rowRangeStart()).isZero();
        assertThat(segment.globalIndexMeta().rowRangeEnd()).isEqualTo(3);
        PrimaryKeyIndexSourceMeta sourceMeta = PrimaryKeyIndexSourceMeta.fromIndexFile(segment);
        BitmapDeletionVector data2Deletes = new BitmapDeletionVector();
        data2Deletes.delete(0);
        Map<String, org.apache.paimon.deletionvectors.DeletionVector> deletionVectors =
                new HashMap<>();
        deletionVectors.put("data-2", data2Deletes);
        Map<String, List<Range>> rowRangesByFile = new HashMap<>();
        rowRangesByFile.put("data-1", Collections.singletonList(new Range(1, 1)));
        rowRangesByFile.put("data-2", Collections.singletonList(new Range(1, 1)));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<PkVectorSearchResult> candidates;
        try {
            candidates =
                    new PkVectorAnnSegmentSearcher(
                                    fileIO, annFile, vectorField(), indexOptions(), "l2", executor)
                            .search(
                                    segment,
                                    sourceMeta,
                                    new float[] {0, 0},
                                    3,
                                    deletionVectors,
                                    new HashSet<>(Arrays.asList("data-1", "data-2")),
                                    rowRangesByFile,
                                    Collections.emptyMap());
        } finally {
            executor.shutdownNow();
        }

        assertThat(candidates)
                .extracting(PkVectorSearchResult::dataFileName, PkVectorSearchResult::rowPosition)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("data-2", 1L),
                        org.assertj.core.groups.Tuple.tuple("data-1", 1L));
    }

    @Test
    void testSearchFiltersInactiveSources() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = annFile(fileIO);
        IndexFileMeta segment =
                annFile.build(
                        Arrays.asList(
                                new PkVectorAnnSegmentFile.Source(
                                        dataFile("retired", 2),
                                        new ArrayReader(new float[][] {{0, 0}, {1, 0}})),
                                new PkVectorAnnSegmentFile.Source(
                                        dataFile("active", 2),
                                        new ArrayReader(new float[][] {{2, 0}, {3, 0}}))),
                        vectorField(),
                        indexOptions(),
                        "l2",
                        "test-vector-ann");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<PkVectorSearchResult> candidates;
        try {
            candidates =
                    new PkVectorAnnSegmentSearcher(
                                    fileIO, annFile, vectorField(), indexOptions(), "l2", executor)
                            .search(
                                    segment,
                                    PrimaryKeyIndexSourceMeta.fromIndexFile(segment),
                                    new float[] {0, 0},
                                    3,
                                    Collections.emptyMap(),
                                    Collections.singleton("active"),
                                    Collections.emptyMap());
        } finally {
            executor.shutdownNow();
        }

        assertThat(candidates)
                .extracting(PkVectorSearchResult::dataFileName)
                .containsExactly("active", "active");
    }

    @Test
    void testRejectsSourcesWithoutRows() {
        LocalFileIO fileIO = LocalFileIO.create();

        assertThatThrownBy(
                        () ->
                                annFile(fileIO)
                                        .build(
                                                Collections.singletonList(
                                                        new PkVectorAnnSegmentFile.Source(
                                                                dataFile("empty", 0),
                                                                new ArrayReader(new float[0][]))),
                                                vectorField(),
                                                indexOptions(),
                                                "l2",
                                                "test-vector-ann"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one source row");
    }

    private PkVectorAnnSegmentFile annFile(LocalFileIO fileIO) {
        return new PkVectorAnnSegmentFile(fileIO, pathFactory());
    }

    private static DataField vectorField() {
        return new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
    }

    private static Options indexOptions() {
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        return options;
    }

    private static DataFileMeta dataFile(String fileName, long rowCount) {
        return DataFileMeta.forAppend(
                fileName,
                100,
                rowCount,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                FileSource.COMPACT,
                null,
                null,
                null,
                null);
    }

    private IndexPathFactory pathFactory() {
        Path directory = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(directory, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(directory, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    private static class ArrayReader implements PkVectorReader {

        private final float[][] vectors;
        private int position;

        private ArrayReader(float[][] vectors) {
            this.vectors = vectors;
        }

        @Override
        public int dimension() {
            return 2;
        }

        @Override
        public long rowCount() {
            return vectors.length;
        }

        @Override
        public boolean readNextVector(float[] reuse) {
            float[] vector = vectors[position++];
            if (vector == null) {
                return false;
            }
            System.arraycopy(vector, 0, reuse, 0, reuse.length);
            return true;
        }

        @Override
        public void close() throws IOException {}
    }
}
