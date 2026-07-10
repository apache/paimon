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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.paimon.index.pkvector.PkVectorSegmentMeta.OrdinalLayout.ROW_POSITION;
import static org.apache.paimon.index.pkvector.PkVectorSegmentMeta.Role.ANN;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests ANN payload construction through the vector GlobalIndexer SPI. */
class PkVectorAnnSegmentFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testBuildsSingleSourceAnnSegmentWithRowPositionOrdinals() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        Path rawPath = new Path(tempPath.resolve("raw").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, rawPath, 2)) {
            writer.write(new float[] {0, 0});
            writer.write(new float[] {2, 0});
        }
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        DataField vectorField = new DataField(7, "embedding", DataTypes.ARRAY(DataTypes.FLOAT()));

        IndexFileMeta segment;
        try (RawVectorSidecarReader rawReader = new RawVectorSidecarReader(fileIO, rawPath)) {
            segment =
                    new PkVectorAnnSegmentFile(fileIO, pathFactory)
                            .buildSingleSource(
                                    dataFile("data-1"),
                                    rawReader,
                                    vectorField,
                                    options,
                                    "definition",
                                    "ARRAY<FLOAT>",
                                    "l2",
                                    "test-vector-ann",
                                    new byte[] {1, 2},
                                    42,
                                    position -> false);
        }

        assertThat(segment.indexType()).isEqualTo(PkVectorAnnSegmentFile.PK_VECTOR_ANN);
        assertThat(segment.rowCount()).isEqualTo(2);
        assertThat(fileIO.exists(pathFactory.toPath(segment))).isTrue();
        PkVectorSegmentMeta metadata =
                PkVectorSegmentMeta.deserialize(segment.globalIndexMeta().indexMeta());
        assertThat(metadata.role()).isEqualTo(ANN);
        assertThat(metadata.ordinalLayout()).isEqualTo(ROW_POSITION);
        assertThat(metadata.sourceFiles()).hasSize(1);
        assertThat(metadata.sourceFiles().get(0).fileName()).isEqualTo("data-1");
        assertThat(metadata.liveRowCountAtBuild()).isEqualTo(2);
        assertThat(metadata.buildSnapshotId()).isEqualTo(42);
        assertThat(metadata.optionsHash()).containsExactly(1, 2);
    }

    @Test
    void testBuildSkipsNullAndSnapshotDeletedRows() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        Path rawPath = new Path(tempPath.resolve("raw-with-deletes").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, rawPath, 2)) {
            writer.write(new float[] {0, 0});
            writer.write(null);
            writer.write(new float[] {2, 0});
        }
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        DataField vectorField = new DataField(7, "embedding", DataTypes.ARRAY(DataTypes.FLOAT()));

        IndexFileMeta segment;
        try (RawVectorSidecarReader rawReader = new RawVectorSidecarReader(fileIO, rawPath)) {
            segment =
                    new PkVectorAnnSegmentFile(fileIO, pathFactory)
                            .buildSingleSource(
                                    dataFile("data-1", 3),
                                    rawReader,
                                    vectorField,
                                    options,
                                    "definition",
                                    "ARRAY<FLOAT>",
                                    "l2",
                                    "test-vector-ann",
                                    new byte[] {1, 2},
                                    42,
                                    position -> position == 0);
        }

        PkVectorSegmentMeta metadata =
                PkVectorSegmentMeta.deserialize(segment.globalIndexMeta().indexMeta());
        assertThat(metadata.liveRowCountAtBuild()).isEqualTo(1);
        assertThat(segment.rowCount()).isEqualTo(1);
    }

    @Test
    void testAnnSearchUsesRowPositionDeletionMask() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        Path rawPath = new Path(tempPath.resolve("ann-search-raw").toUri());
        try (RawVectorSidecarWriter writer = new RawVectorSidecarWriter(fileIO, rawPath, 2)) {
            writer.write(new float[] {0, 0});
            writer.write(new float[] {1, 0});
            writer.write(new float[] {2, 0});
        }
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        DataField vectorField = new DataField(7, "embedding", DataTypes.ARRAY(DataTypes.FLOAT()));
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory);
        IndexFileMeta segment;
        try (RawVectorSidecarReader rawReader = new RawVectorSidecarReader(fileIO, rawPath)) {
            segment =
                    annFile.buildSingleSource(
                            dataFile("data-1", 3),
                            rawReader,
                            vectorField,
                            options,
                            "definition",
                            "ARRAY<FLOAT>",
                            "l2",
                            "test-vector-ann",
                            new byte[] {1, 2},
                            42,
                            position -> false);
        }
        PkVectorSegmentMeta metadata =
                PkVectorSegmentMeta.deserialize(segment.globalIndexMeta().indexMeta());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        BitmapDeletionVector deletionVector = new BitmapDeletionVector();
        deletionVector.delete(0);
        List<PkVectorAnnSegmentSearcher.Candidate> candidates;
        try {
            candidates =
                    new PkVectorAnnSegmentSearcher(fileIO, annFile, vectorField, options, executor)
                            .search(
                                    segment,
                                    metadata,
                                    new float[] {0, 0},
                                    2,
                                    deletionVector,
                                    Collections.emptyMap());
        } finally {
            executor.shutdownNow();
        }

        assertThat(candidates)
                .extracting(PkVectorAnnSegmentSearcher.Candidate::rowPosition)
                .containsExactly(1L, 2L);
        assertThat(candidates)
                .extracting(PkVectorAnnSegmentSearcher.Candidate::distance)
                .containsExactly(1F, 4F);
    }

    @Test
    void testBuildsAndSearchesMultiSourceAnnSegment() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        IndexPathFactory pathFactory = pathFactory();
        Path raw1Path = new Path(tempPath.resolve("multi-raw-1").toUri());
        Path raw2Path = new Path(tempPath.resolve("multi-raw-2").toUri());
        try (RawVectorSidecarWriter writer1 = new RawVectorSidecarWriter(fileIO, raw1Path, 2);
                RawVectorSidecarWriter writer2 = new RawVectorSidecarWriter(fileIO, raw2Path, 2)) {
            writer1.write(new float[] {5, 0});
            writer1.write(new float[] {10, 0});
            writer2.write(new float[] {0, 0});
            writer2.write(new float[] {2, 0});
        }
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        DataField vectorField = new DataField(7, "embedding", DataTypes.ARRAY(DataTypes.FLOAT()));
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory);
        IndexFileMeta segment;
        try (RawVectorSidecarReader raw1 = new RawVectorSidecarReader(fileIO, raw1Path);
                RawVectorSidecarReader raw2 = new RawVectorSidecarReader(fileIO, raw2Path)) {
            segment =
                    annFile.buildMultiSource(
                            Arrays.asList(
                                    new PkVectorAnnSegmentFile.Source(dataFile("data-1"), raw1),
                                    new PkVectorAnnSegmentFile.Source(dataFile("data-2"), raw2)),
                            vectorField,
                            options,
                            "definition",
                            "ARRAY<FLOAT>",
                            "l2",
                            "test-vector-ann",
                            new byte[] {1, 2},
                            42);
        }

        PkVectorSegmentMeta metadata =
                PkVectorSegmentMeta.deserialize(segment.globalIndexMeta().indexMeta());
        assertThat(metadata.ordinalLayout())
                .isEqualTo(PkVectorSegmentMeta.OrdinalLayout.FILE_POSITION);
        assertThat(metadata.sourceFiles())
                .extracting(PkVectorSegmentMeta.SourceFile::fileName)
                .containsExactly("data-1", "data-2");

        BitmapDeletionVector data2Deletes = new BitmapDeletionVector();
        data2Deletes.delete(0);
        Map<String, org.apache.paimon.deletionvectors.DeletionVector> deletionVectors =
                new HashMap<>();
        deletionVectors.put("data-2", data2Deletes);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<PkVectorAnnSegmentSearcher.Candidate> candidates;
        try {
            candidates =
                    new PkVectorAnnSegmentSearcher(fileIO, annFile, vectorField, options, executor)
                            .search(
                                    segment,
                                    metadata,
                                    new float[] {0, 0},
                                    3,
                                    deletionVectors,
                                    Collections.emptyMap());
        } finally {
            executor.shutdownNow();
        }

        assertThat(candidates)
                .extracting(
                        PkVectorAnnSegmentSearcher.Candidate::dataFileName,
                        PkVectorAnnSegmentSearcher.Candidate::rowPosition)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("data-2", 1L),
                        org.assertj.core.groups.Tuple.tuple("data-1", 0L),
                        org.assertj.core.groups.Tuple.tuple("data-1", 1L));
        assertThat(candidates.get(0).distance())
                .isCloseTo(4F, org.assertj.core.data.Offset.offset(0.001F));
        assertThat(candidates.get(1).distance())
                .isCloseTo(25F, org.assertj.core.data.Offset.offset(0.001F));
        assertThat(candidates.get(2).distance())
                .isCloseTo(100F, org.assertj.core.data.Offset.offset(0.001F));
    }

    private static DataFileMeta dataFile(String fileName) {
        return dataFile(fileName, 2);
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
                FileSource.APPEND,
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
}
