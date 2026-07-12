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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests ANN maintenance over compact data-file sources. */
class BucketedVectorIndexMaintainerTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testPartialCompactionRebuildsMultiSourceAnnFromDataFiles() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        Options options = indexOptions();
        DataFileMeta data1 = dataFile("data-1");
        DataFileMeta data2 = dataFile("data-2");
        DataFileMeta data3 = dataFile("data-3");
        IndexFileMeta initialAnn =
                annFile.build(
                        Arrays.asList(
                                new PkVectorAnnSegmentFile.Source(
                                        data1, new ArrayReader(new float[][] {{1, 0}})),
                                new PkVectorAnnSegmentFile.Source(
                                        data2, new ArrayReader(new float[][] {{2, 0}}))),
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader reader2 = reader(new float[][] {{2, 0}});
        PkVectorDataFileReader reader3 = reader(new float[][] {{3, 0}});
        when(readerFactory.create(data2)).thenReturn(reader2);
        when(readerFactory.create(data3)).thenReturn(reader3);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        options,
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Arrays.asList(data1, data2),
                        Collections.singletonList(initialAnn));

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.singletonList(data1),
                                Collections.singletonList(data3),
                                Collections.emptyList()));

        BucketedVectorIndexMaintainer.VectorIndexIncrement increment =
                commit.compactIncrement().get();
        assertThat(increment.deletedIndexFiles()).containsExactly(initialAnn);
        assertThat(increment.newIndexFiles()).hasSize(1);
        IndexFileMeta repaired = increment.newIndexFiles().get(0);
        assertThat(repaired.indexType()).isEqualTo("test-vector-ann");
        assertThat(PkVectorSourceMeta.fromIndexFile(repaired).sourceFiles())
                .extracting(PkVectorSourceFile::fileName)
                .containsExactly("data-2", "data-3");
    }

    @Test
    void testBuildsAnnForSingleCompactSource() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkVectorAnnSegmentFile annFile = new PkVectorAnnSegmentFile(fileIO, pathFactory());
        DataField vectorField =
                new DataField(7, "embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()));
        DataFileMeta data = dataFile("data");
        PkVectorDataFileReader.Factory readerFactory = mock(PkVectorDataFileReader.Factory.class);
        PkVectorDataFileReader dataReader = reader(new float[][] {{1, 0}});
        when(readerFactory.create(data)).thenReturn(dataReader);
        BucketedVectorIndexMaintainer maintainer =
                new BucketedVectorIndexMaintainer(
                        7,
                        annFile,
                        vectorField,
                        indexOptions(),
                        "l2",
                        "test-vector-ann",
                        readerFactory,
                        Collections.emptyList(),
                        Collections.emptyList());

        BucketedVectorIndexMaintainer.VectorIndexCommit commit =
                maintainer.prepareCommit(
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(data),
                                Collections.emptyList()));

        assertThat(commit.compactIncrement()).isPresent();
        assertThat(commit.compactIncrement().get().newIndexFiles()).hasSize(1);
    }

    private static Options indexOptions() {
        Options options = new Options();
        options.setString("test.vector.dimension", "2");
        options.setString("test.vector.metric", "l2");
        return options;
    }

    private static PkVectorDataFileReader reader(float[][] vectors) throws IOException {
        PkVectorDataFileReader reader = mock(PkVectorDataFileReader.class);
        when(reader.dimension()).thenReturn(2);
        when(reader.rowCount()).thenReturn((long) vectors.length);
        final int[] position = {0};
        when(reader.readNextVector(org.mockito.ArgumentMatchers.any(float[].class)))
                .thenAnswer(
                        invocation -> {
                            float[] vector = vectors[position[0]++];
                            System.arraycopy(
                                    vector, 0, invocation.getArgument(0), 0, vector.length);
                            return true;
                        });
        return reader;
    }

    private static DataFileMeta dataFile(String fileName) {
        return DataFileMeta.forAppend(
                        fileName,
                        100,
                        1,
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
                        null)
                .upgrade(1);
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
            System.arraycopy(vector, 0, reuse, 0, vector.length);
            return true;
        }

        @Override
        public void close() throws IOException {}
    }
}
