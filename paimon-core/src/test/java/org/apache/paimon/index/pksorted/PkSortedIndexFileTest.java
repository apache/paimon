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
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests source-backed sorted index payload creation. */
class PkSortedIndexFileTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testHasSingleBuildEntryPoint() {
        List<Method> buildMethods =
                Arrays.stream(PkSortedIndexFile.class.getDeclaredMethods())
                        .filter(method -> method.getName().equals("build"))
                        .collect(Collectors.toList());
        assertThat(buildMethods).hasSize(1);
        assertThat(buildMethods.get(0).getReturnType()).isEqualTo(IndexFileMeta.class);
    }

    @Test
    void testUsesCloseableStreamingBitmapWriter() throws Exception {
        PkSortedIndexFile indexFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory(tempPath));
        GlobalIndexSingleColumnWriter writer =
                indexFile.createWriter(
                        "bitmap",
                        field(),
                        options(),
                        new GlobalIndexFileWriter() {
                            @Override
                            public String newFileName(String prefix) {
                                return prefix + ".index";
                            }

                            @Override
                            public PositionOutputStream newOutputStream(String fileName) {
                                throw new AssertionError("Writer must open output lazily.");
                            }
                        });

        assertThat(writer).isInstanceOf(AutoCloseable.class);
        ((AutoCloseable) writer).close();
    }

    @Test
    void testBuildsSingleBTreeAndBitmapPayload() throws Exception {
        for (String indexType : Arrays.asList("btree", "bitmap")) {
            java.nio.file.Path indexDirectory = Files.createDirectory(tempPath.resolve(indexType));
            PkSortedIndexFile indexFile =
                    new PkSortedIndexFile(LocalFileIO.create(), pathFactory(indexDirectory));
            PrimaryKeyIndexSourceFile source = new PrimaryKeyIndexSourceFile("data-file", 3);

            IndexFileMeta payload =
                    indexFile.build(
                            Collections.singletonList(source),
                            field(),
                            indexType,
                            options(),
                            Arrays.asList(
                                            new PkSortedIndexFile.Entry(null, 1),
                                            new PkSortedIndexFile.Entry(10, 2),
                                            new PkSortedIndexFile.Entry(20, 0))
                                    .iterator());

            assertThat(payload.indexType()).isEqualTo(indexType);
            assertThat(payload.rowCount()).isEqualTo(3L);
            GlobalIndexMeta meta = payload.globalIndexMeta();
            assertThat(meta.rowRangeStart()).isZero();
            assertThat(meta.rowRangeEnd()).isEqualTo(2);
            assertThat(meta.indexFieldId()).isEqualTo(7);
            assertThat(meta.indexMeta()).isNotEmpty();
            assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFile())
                    .isEqualTo(source);
            assertThat(indexFile.exists(payload)).isTrue();
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

        IndexFileMeta payload =
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

        assertThat(payload.rowCount()).isEqualTo(5L);
        assertThat(payload.globalIndexMeta().rowRangeStart()).isZero();
        assertThat(payload.globalIndexMeta().rowRangeEnd()).isEqualTo(4);
        assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles())
                .containsExactlyElementsOf(sources);
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
                                        Collections.singletonList(
                                                new PrimaryKeyIndexSourceFile("data-file", 2)),
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

    @Test
    void testClosesWriterAfterFailedBuild() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        class CloseableWriter implements GlobalIndexSingleColumnWriter, AutoCloseable {

            @Override
            public void write(Object key, long relativeRowId) {}

            @Override
            public List<ResultEntry> finish() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        }

        PkSortedIndexFile indexFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory(tempPath)) {
                    @Override
                    protected GlobalIndexSingleColumnWriter createWriter(
                            String indexType,
                            DataField indexField,
                            Options indexOptions,
                            GlobalIndexFileWriter fileWriter) {
                        return new CloseableWriter();
                    }
                };

        assertThatThrownBy(
                        () ->
                                indexFile.build(
                                        Collections.singletonList(
                                                new PrimaryKeyIndexSourceFile("data-file", 1)),
                                        field(),
                                        "bitmap",
                                        options(),
                                        Collections.singletonList(
                                                        new PkSortedIndexFile.Entry(10, 1))
                                                .iterator()))
                .hasMessageContaining("outside sorted index group row range");
        assertThat(closed).isTrue();
    }

    private static DataField field() {
        return new DataField(7, "indexed", DataTypes.INT());
    }

    private static Options options() {
        return new Options();
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
