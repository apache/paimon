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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests physical source sorting and payload construction. */
class PkSortedIndexBuilderTest {

    @TempDir java.nio.file.Path tempPath;

    @Test
    void testBuildsQueryableBTreeAndBitmapFromUnsortedPhysicalRows() throws Exception {
        List<PkSortedDataFileReader.Entry> entries =
                Arrays.asList(entry(20, 0), entry(null, 1), entry(10, 2), entry(20, 3));
        DataFileMeta source = dataFile("data-file", entries.size());

        for (String indexType : Arrays.asList("btree", "bitmap")) {
            java.nio.file.Path directory = FilesHelper.createDirectory(tempPath, indexType);
            LocalFileIO fileIO = LocalFileIO.create();
            IndexPathFactory pathFactory = pathFactory(directory);
            Options options = options();
            IOManager ioManager = IOManager.create(directory.resolve("spill").toString());
            List<IndexFileMeta> payloads;
            try {
                payloads =
                        new PkSortedIndexBuilder(
                                        ignored -> new ArrayReader(entries),
                                        new PkSortedIndexFile(fileIO, pathFactory),
                                        field(),
                                        indexType,
                                        options,
                                        ioManager)
                                .build(source);
            } finally {
                ioManager.close();
            }

            assertThat(payloads).hasSize(1);
            assertQuery(fileIO, pathFactory, indexType, options, payloads, false, 20, 0L, 3L);
            assertQuery(fileIO, pathFactory, indexType, options, payloads, true, null, 1L);
        }
    }

    @Test
    void testBuildsSeveralSourcesInDeterministicOrdinalOrder() throws Exception {
        DataFileMeta sourceB = dataFile("data-b", 3);
        DataFileMeta sourceA = dataFile("data-a", 2);
        List<PrimaryKeyIndexSourceFile> capturedSources = new ArrayList<>();
        List<PkSortedIndexFile.Entry> capturedEntries = new ArrayList<>();
        PkSortedIndexFile capturingFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory(tempPath)) {
                    @Override
                    public List<IndexFileMeta> build(
                            List<PrimaryKeyIndexSourceFile> sourceFiles,
                            DataField indexField,
                            String indexType,
                            Options indexOptions,
                            Iterator<Entry> sortedEntries) {
                        capturedSources.addAll(sourceFiles);
                        sortedEntries.forEachRemaining(capturedEntries::add);
                        return Collections.emptyList();
                    }
                };

        IOManager ioManager = IOManager.create(tempPath.resolve("multi-spill").toString());
        try {
            new PkSortedIndexBuilder(
                            dataFile ->
                                    new ArrayReader(
                                            dataFile.fileName().equals("data-a")
                                                    ? Arrays.asList(entry(3, 0), entry(0, 1))
                                                    : Arrays.asList(
                                                            entry(1, 0), entry(4, 1), entry(2, 2))),
                            capturingFile,
                            field(),
                            "btree",
                            options(),
                            ioManager)
                    .build(Arrays.asList(sourceB, sourceA));
        } finally {
            ioManager.close();
        }

        assertThat(capturedSources)
                .extracting(PrimaryKeyIndexSourceFile::fileName)
                .containsExactly("data-a", "data-b");
        assertThat(capturedEntries)
                .extracting(PkSortedIndexFile.Entry::value)
                .containsExactly(0, 1, 2, 3, 4);
        assertThat(capturedEntries)
                .extracting(PkSortedIndexFile.Entry::rowId)
                .containsExactly(1L, 2L, 4L, 0L, 3L);
    }

    @Test
    void testForcedSpillSortsRowsAndClosesTaskOwnedIoManager() throws Exception {
        int rowCount = 20_000;
        List<PkSortedDataFileReader.Entry> entries = new ArrayList<>(rowCount);
        for (int position = 0; position < rowCount; position++) {
            entries.add(entry(rowCount - position - 1, position));
        }
        List<Integer> sortedValues = new ArrayList<>(rowCount);
        TrackingIOManager ioManager =
                new TrackingIOManager(tempPath.resolve("owned-spill").toString());
        PkSortedIndexFile capturingFile =
                new PkSortedIndexFile(LocalFileIO.create(), pathFactory(tempPath)) {
                    @Override
                    public List<IndexFileMeta> build(
                            List<PrimaryKeyIndexSourceFile> sourceFiles,
                            DataField indexField,
                            String indexType,
                            Options indexOptions,
                            Iterator<Entry> sortedEntries) {
                        while (sortedEntries.hasNext()) {
                            sortedValues.add((Integer) sortedEntries.next().value());
                        }
                        return Collections.emptyList();
                    }
                };
        Options options = options();
        options.set(
                CoreOptions.WRITE_BUFFER_SIZE,
                org.apache.paimon.options.MemorySize.parse("128 kb"));
        options.set(CoreOptions.PAGE_SIZE, org.apache.paimon.options.MemorySize.parse("32 kb"));

        new PkSortedIndexBuilder(
                ignored -> new ArrayReader(entries),
                capturingFile,
                field(),
                "btree",
                options,
                null) {
            @Override
            protected IOManager createTemporaryIOManager() {
                return ioManager;
            }
        }.build(dataFile("large-data-file", rowCount));

        assertThat(sortedValues)
                .containsExactlyElementsOf(
                        LongStream.range(0, rowCount)
                                .mapToObj(value -> (int) value)
                                .collect(Collectors.toList()));
        assertThat(ioManager.createdSpillWriters).isGreaterThan(0);
        assertThat(ioManager.closed).isTrue();
    }

    private static void assertQuery(
            LocalFileIO fileIO,
            IndexPathFactory pathFactory,
            String indexType,
            Options options,
            List<IndexFileMeta> payloads,
            boolean isNull,
            Object literal,
            Long... expected)
            throws Exception {
        List<GlobalIndexIOMeta> ioMetas = new ArrayList<>();
        for (IndexFileMeta payload : payloads) {
            ioMetas.add(
                    new GlobalIndexIOMeta(
                            pathFactory.toPath(payload.fileName()),
                            payload.fileSize(),
                            payload.globalIndexMeta().indexMeta()));
        }
        ExecutorService executor = newDirectExecutorService();
        try (GlobalIndexReader reader =
                GlobalIndexer.create(indexType, field(), options)
                        .createReader(
                                new GlobalIndexFileReadWrite(fileIO, pathFactory),
                                ioMetas,
                                executor)) {
            FieldRef fieldRef = new FieldRef(7, "indexed", DataTypes.INT());
            GlobalIndexResult result =
                    (isNull ? reader.visitIsNull(fieldRef) : reader.visitEqual(fieldRef, literal))
                            .join()
                            .get();
            assertThat(result.results()).containsExactlyInAnyOrder(expected);
        } finally {
            executor.shutdownNow();
        }
    }

    private static PkSortedDataFileReader.Entry entry(Object value, long position) {
        return new PkSortedDataFileReader.Entry(value, position);
    }

    private static DataField field() {
        return new DataField(7, "indexed", DataTypes.INT());
    }

    private static Options options() {
        Options options = new Options();
        options.set(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE, 2L);
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

    private static final class ArrayReader implements PkSortedIndexBuilder.Reader {

        private final List<PkSortedDataFileReader.Entry> entries;
        private int position;

        private ArrayReader(List<PkSortedDataFileReader.Entry> entries) {
            this.entries = entries;
        }

        @Override
        public long rowCount() {
            return entries.size();
        }

        @Override
        public PkSortedDataFileReader.Entry readNext() {
            return position == entries.size() ? null : entries.get(position++);
        }

        @Override
        public void close() {}
    }

    private static final class TrackingIOManager extends IOManagerImpl {

        private int createdSpillWriters;
        private boolean closed;

        private TrackingIOManager(String tempDir) {
            super(tempDir);
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID)
                throws IOException {
            createdSpillWriters++;
            return super.createBufferFileWriter(channelID);
        }

        @Override
        public void close() throws Exception {
            closed = true;
            super.close();
        }
    }

    private static final class FilesHelper {

        private static java.nio.file.Path createDirectory(java.nio.file.Path parent, String child)
                throws IOException {
            return java.nio.file.Files.createDirectory(parent.resolve(child));
        }
    }
}
