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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests source-order streaming for sequential primary-key indexes. */
class PkSequentialIndexBuilderTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testStreamsFilesAndRowsInCanonicalSourceOrder() throws Exception {
        DataFileMeta sourceB = dataFile("data-b", 1);
        DataFileMeta sourceA = dataFile("data-a", 2);
        List<PrimaryKeyIndexSourceFile> capturedSources = new ArrayList<>();
        List<PkSortedIndexFile.Entry> capturedEntries = new ArrayList<>();
        List<ArrayReader> readers = new ArrayList<>();
        PkSortedIndexFile capturingFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory()) {
                    @Override
                    public IndexFileMeta build(
                            int dataLevel,
                            List<PrimaryKeyIndexSourceFile> sourceFiles,
                            DataField indexField,
                            String indexType,
                            Options indexOptions,
                            Iterator<Entry> entries) {
                        capturedSources.addAll(sourceFiles);
                        entries.forEachRemaining(capturedEntries::add);
                        return ignoredPayload();
                    }
                };

        new PkSequentialIndexBuilder(
                        dataFile -> {
                            ArrayReader reader =
                                    new ArrayReader(
                                            dataFile.fileName().equals("data-a")
                                                    ? Arrays.asList(entry("a", 0), entry(null, 1))
                                                    : Collections.singletonList(entry("b", 0)));
                            readers.add(reader);
                            return reader;
                        },
                        capturingFile,
                        field(),
                        "fmindex",
                        new Options())
                .build(Arrays.asList(sourceB, sourceA));

        assertThat(capturedSources)
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-a", "data-b");
        assertThat(capturedEntries)
                .extracting(PkSortedIndexFile.Entry::value)
                .containsExactly("a", null, "b");
        assertThat(capturedEntries)
                .extracting(PkSortedIndexFile.Entry::rowId)
                .containsExactly(0L, 1L, 2L);
        assertThat(readers).allMatch(ArrayReader::isClosed);
    }

    @Test
    void testRejectsNonConsecutivePhysicalPositionsAndClosesReader() {
        ArrayReader reader =
                new ArrayReader(
                        Arrays.asList(
                                entry(BinaryString.fromString("a"), 0),
                                entry(BinaryString.fromString("b"), 2)));
        PkSequentialIndexBuilder builder =
                new PkSequentialIndexBuilder(
                        ignored -> reader,
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        field(),
                        "fmindex",
                        new Options());

        assertThatThrownBy(() -> builder.build(Collections.singletonList(dataFile("data-file", 2))))
                .hasMessageContaining("returned row position 2, expected 1");
        assertThat(reader.isClosed()).isTrue();
    }

    @Test
    void testPropagatesReaderCreationIOException() {
        PkSequentialIndexBuilder builder =
                new PkSequentialIndexBuilder(
                        (PkSortedIndexBuilder.ReaderFactory)
                                ignored -> {
                                    throw new IOException("expected reader failure");
                                },
                        new PkSortedIndexFile(LocalFileIO.create(), pathFactory()),
                        field(),
                        "fmindex",
                        new Options());

        assertThatThrownBy(() -> builder.build(Collections.singletonList(dataFile("data-file", 1))))
                .isInstanceOf(IOException.class)
                .hasMessage("expected reader failure");
    }

    private static PkSortedDataFileReader.Entry entry(@Nullable Object value, long position) {
        return new PkSortedDataFileReader.Entry(value, position);
    }

    private static DataField field() {
        return new DataField(7, "content", DataTypes.STRING());
    }

    private static IndexFileMeta ignoredPayload() {
        return new IndexFileMeta("test", "test", 0, 0, (GlobalIndexMeta) null, null);
    }

    private static DataFileMeta dataFile(String name, long rowCount) {
        return DataFileMeta.forAppend(
                        name,
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
                        null)
                .upgrade(1);
    }

    private IndexPathFactory pathFactory() {
        Path root = new Path(tempPath.toUri());
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(root, fileName);
            }

            @Override
            public Path newPath() {
                return new Path(root, UUID.randomUUID().toString());
            }

            @Override
            public boolean isExternalPath() {
                return false;
            }
        };
    }

    private static final class ArrayReader implements PkSortedIndexBuilder.Reader {

        private final List<PkSortedDataFileReader.Entry> entries;
        private int position;
        private boolean closed;

        private ArrayReader(List<PkSortedDataFileReader.Entry> entries) {
            this.entries = entries;
        }

        @Override
        public long rowCount() {
            return entries.size();
        }

        @Nullable
        @Override
        public PkSortedDataFileReader.Entry readNext() {
            return position == entries.size() ? null : entries.get(position++);
        }

        @Override
        public void close() {
            closed = true;
        }

        private boolean isClosed() {
            return closed;
        }
    }
}
