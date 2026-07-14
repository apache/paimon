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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests source-backed sorted index payload creation. */
class PkSortedIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testBuildsSingleBTreeAndBitmapPayloadIgnoringRecordsPerRange() throws Exception {
        for (String indexType : Arrays.asList("btree", "bitmap")) {
            java.nio.file.Path indexDirectory = Files.createDirectory(tempPath.resolve(indexType));
            PkSortedIndexFile indexFile =
                    new PkSortedIndexFile(LocalFileIO.create(), pathFactory(indexDirectory));
            PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-file", 3);

            List<IndexFileMeta> payloads =
                    indexFile.build(
                            source,
                            field(),
                            indexType,
                            options(),
                            Arrays.asList(
                                            new PkSortedIndexFile.Entry(null, 1),
                                            new PkSortedIndexFile.Entry(10, 2),
                                            new PkSortedIndexFile.Entry(20, 0))
                                    .iterator());

            assertThat(payloads).hasSize(1);
            assertThat(payloads).extracting(IndexFileMeta::indexType).containsOnly(indexType);
            assertThat(payloads).extracting(IndexFileMeta::rowCount).containsExactly(3L);
            assertThat(payloads)
                    .allSatisfy(
                            payload -> {
                                GlobalIndexMeta meta = payload.globalIndexMeta();
                                assertThat(meta.rowRangeStart()).isZero();
                                assertThat(meta.rowRangeEnd()).isEqualTo(2);
                                assertThat(meta.indexFieldId()).isEqualTo(7);
                                assertThat(meta.indexMeta()).isNotEmpty();
                                assertThat(
                                                PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                                        .sourceFile())
                                        .isEqualTo(source);
                                assertThat(indexFile.exists(payload)).isTrue();
                            });
        }
    }

    @Test
    void testBuildsMultiSourcePayloadsInOneOrdinalDomain() throws Exception {
        PkSortedIndexFile indexFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory(tempPath));
        List<PrimaryKeyIndexSourceFile> sources =
                Arrays.asList(
                        new PrimaryKeyIndexSourceFile("data-a", 2),
                        new PrimaryKeyIndexSourceFile("data-b", 3));

        List<IndexFileMeta> payloads =
                indexFile.build(
                        sources,
                        field(),
                        "btree",
                        options(),
                        Arrays.asList(
                                        new PkSortedIndexFile.Entry(null, 3),
                                        new PkSortedIndexFile.Entry(10, 1),
                                        new PkSortedIndexFile.Entry(20, 4),
                                        new PkSortedIndexFile.Entry(30, 0),
                                        new PkSortedIndexFile.Entry(40, 2))
                                .iterator());

        assertThat(payloads).extracting(IndexFileMeta::rowCount).containsExactly(5L);
        assertThat(payloads)
                .allSatisfy(
                        payload -> {
                            assertThat(payload.globalIndexMeta().rowRangeStart()).isZero();
                            assertThat(payload.globalIndexMeta().rowRangeEnd()).isEqualTo(4);
                            assertThat(
                                            PrimaryKeyIndexSourceMeta.fromIndexFile(payload)
                                                    .sourceFiles())
                                    .containsExactlyElementsOf(sources);
                        });
    }

    @Test
    void testRejectsMultiplePayloadsAndDeletesWholeGroup() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        PkSortedIndexFile indexFile =
                new PkSortedIndexFile(fileIO, pathFactory(tempPath)) {
                    @Override
                    protected GlobalIndexSingleColumnWriter createWriter(
                            String indexType,
                            DataField indexField,
                            Options indexOptions,
                            GlobalIndexFileWriter fileWriter) {
                        return new GlobalIndexSingleColumnWriter() {
                            private long rowCount;

                            @Override
                            public void write(Object key, long relativeRowId) {
                                rowCount++;
                            }

                            @Override
                            public List<ResultEntry> finish() {
                                List<ResultEntry> results = new ArrayList<>();
                                for (int i = 0; i < 2; i++) {
                                    String fileName = fileWriter.newFileName("test");
                                    try (PositionOutputStream output =
                                            fileWriter.newOutputStream(fileName)) {
                                        output.write(new byte[] {1});
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    results.add(
                                            new ResultEntry(fileName, rowCount, new byte[] {2}));
                                }
                                return results;
                            }
                        };
                    }
                };

        assertThatThrownBy(
                        () ->
                                indexFile.build(
                                        new PrimaryKeyIndexSourceFile("data-file", 2),
                                        field(),
                                        "btree",
                                        options(),
                                        Arrays.asList(
                                                        new PkSortedIndexFile.Entry(10, 0),
                                                        new PkSortedIndexFile.Entry(20, 1))
                                                .iterator()))
                .hasMessageContaining("must produce exactly one payload file");

        try (Stream<java.nio.file.Path> files = Files.list(tempPath)) {
            assertThat(files).isEmpty();
        }
    }

    private static DataField field() {
        return new DataField(7, "indexed", DataTypes.INT());
    }

    private static Options options() {
        Options options = new Options();
        options.set(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE, 2L);
        return options;
    }

    private static IndexPathFactory pathFactory(java.nio.file.Path directory) {
        Path path = new Path(directory.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(path, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(path, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }
}
