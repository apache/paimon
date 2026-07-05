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

package org.apache.paimon.table.source;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactoryUtils;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Implementation for raw full-text search. */
class RawFullTextReadImpl {

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    private final int limit;
    private final DataField textColumn;
    private final IndexSearch indexSearch;

    RawFullTextReadImpl(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionFilter,
            int limit,
            DataField textColumn,
            IndexSearch indexSearch) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.textColumn = textColumn;
        this.indexSearch = indexSearch;
    }

    ScoredGlobalIndexResult withRawSearch(
            ScoredGlobalIndexResult indexedResult,
            List<Range> rawRowRanges,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            ExecutorService executor) {
        rawRowRanges = Range.sortAndMergeOverlap(rawRowRanges, true);
        if (rawRowRanges.isEmpty()) {
            return indexedResult;
        }

        ScoredGlobalIndexResult rawResult =
                readRawSearch(rawRowRanges, splitsByColumn, executor);
        return overrideWithRawSearch(indexedResult, rawRowRanges, rawResult);
    }

    private ScoredGlobalIndexResult readRawSearch(
            List<Range> rawRowRanges,
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            ExecutorService executor) {
        RowType readType = SpecialFields.rowTypeWithRowId(table.rowType());
        TableScan.Plan plan = rawReadBuilder(readType).withRowRanges(rawRowRanges).newScan().plan();
        ReadBuilder readBuilder = rawReadBuilder(readType);
        int rowIdIndex = readType.getFieldIndex(SpecialFields.ROW_ID.name());
        Map<String, RawFullTextIndex> rawIndexes =
                createRawFullTextIndexes(splitsByColumn, readType, rawRowRanges);

        try {
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan);
                    CloseableIterator<InternalRow> iterator = reader.toCloseableIterator()) {
                while (iterator.hasNext()) {
                    InternalRow row = iterator.next();
                    for (RawFullTextIndex rawIndex : rawIndexes.values()) {
                        long rowId = row.getLong(rowIdIndex);
                        rawIndex.write(row, rowId);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read raw rows for full-text search.", e);
        }

        try {
            Map<String, List<IndexFullTextSearchSplit>> rawSplitsByColumn = new HashMap<>();
            for (Map.Entry<String, RawFullTextIndex> entry : rawIndexes.entrySet()) {
                IndexFullTextSearchSplit split = entry.getValue().finish();
                if (split != null) {
                    rawSplitsByColumn.put(entry.getKey(), Collections.singletonList(split));
                }
            }

            return indexSearch
                    .eval(
                            rawSplitsByColumn,
                            new RawFullTextIndexPathFactory(rawIndexes),
                            m ->
                                    new MemorySeekableInputStream(
                                            rawFileBytes(rawIndexes, m.filePath().getName())),
                            executor)
                    .topK(limit);
        } finally {
            IOUtils.closeAllQuietly(rawIndexes.values());
        }
    }

    private static ScoredGlobalIndexResult overrideWithRawSearch(
            ScoredGlobalIndexResult indexedResult,
            List<Range> rawRowRanges,
            ScoredGlobalIndexResult rawResult) {
        RoaringNavigableMap64 rawRows = new RoaringNavigableMap64();
        for (Range range : rawRowRanges) {
            rawRows.addRange(range);
        }
        RoaringNavigableMap64 filteredIndexedRows =
                RoaringNavigableMap64.or(new RoaringNavigableMap64(), indexedResult.results());
        filteredIndexedRows.andNot(rawRows);
        RoaringNavigableMap64 resultRows =
                RoaringNavigableMap64.or(filteredIndexedRows, rawResult.results());
        return ScoredGlobalIndexResult.create(
                resultRows,
                rowId ->
                        rawResult.results().contains(rowId)
                                ? rawResult.scoreGetter().score(rowId)
                                : indexedResult.scoreGetter().score(rowId));
    }

    private Map<String, RawFullTextIndex> createRawFullTextIndexes(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
            RowType readType,
            List<Range> rawRowRanges) {
        Map<String, RawFullTextIndex> rawIndexes = new HashMap<>();
        long rowRangeStart = rawRowRanges.get(0).from;
        long rowRangeEnd = rawRowRanges.get(rawRowRanges.size() - 1).to;
        String fallbackIndexType = firstIndexType(splitsByColumn);
        String column = textColumn.name();
        String indexType = indexType(column, splitsByColumn);
        if (indexType == null) {
            indexType = checkNotNull(fallbackIndexType);
        }
        GlobalIndexer globalIndexer =
                GlobalIndexerFactoryUtils.load(indexType).create(textColumn, rawSearchOptions());
        try {
            RawFullTextIndexFileWriter fileWriter = new RawFullTextIndexFileWriter(column);
            GlobalIndexWriter indexWriter = globalIndexer.createWriter(fileWriter);
            if (!(indexWriter instanceof GlobalIndexSingleColumnWriter)) {
                throw new IllegalArgumentException(
                        "Full-text raw search requires a single-column global index writer.");
            }
            rawIndexes.put(
                    column,
                    new RawFullTextIndex(
                            column,
                            readColumnIndex(textColumn, readType),
                            indexType,
                            textColumn.id(),
                            rowRangeStart,
                            rowRangeEnd,
                            (GlobalIndexSingleColumnWriter) indexWriter,
                            fileWriter));
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create raw full-text index writer for column: " + column, e);
        }
        return rawIndexes;
    }

    @Nullable
    private static String indexType(
            String column, Map<String, List<IndexFullTextSearchSplit>> splitsByColumn) {
        List<IndexFullTextSearchSplit> splits = splitsByColumn.get(column);
        if (splits == null || splits.isEmpty() || splits.get(0).fullTextIndexFiles().isEmpty()) {
            return null;
        }
        return splits.get(0).fullTextIndexFiles().get(0).indexType();
    }

    @Nullable
    private static String firstIndexType(
            Map<String, List<IndexFullTextSearchSplit>> splitsByColumn) {
        for (List<IndexFullTextSearchSplit> splits : splitsByColumn.values()) {
            if (splits != null
                    && !splits.isEmpty()
                    && !splits.get(0).fullTextIndexFiles().isEmpty()) {
                return splits.get(0).fullTextIndexFiles().get(0).indexType();
            }
        }
        return null;
    }

    private static int readColumnIndex(DataField textColumn, RowType readType) {
        return readType.getFieldIndexByFieldId(textColumn.id());
    }

    private static byte[] rawFileBytes(Map<String, RawFullTextIndex> rawIndexes, String fileName) {
        for (RawFullTextIndex rawIndex : rawIndexes.values()) {
            if (rawIndex.containsFile(fileName)) {
                return rawIndex.fileBytes(fileName);
            }
        }
        throw new IllegalArgumentException("Unknown raw full-text index file: " + fileName);
    }

    private ReadBuilder rawReadBuilder(RowType readType) {
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        if (partitionFilter != null) {
            readBuilder.withPartitionFilter(partitionFilter);
        }
        return readBuilder;
    }

    private Options rawSearchOptions() {
        Options options = new Options(table.coreOptions().toConfiguration().toMap());
        options.setString("full-text.searcher-pool.max-size", "0");
        return options;
    }

    interface IndexSearch {

        ScoredGlobalIndexResult eval(
                Map<String, List<IndexFullTextSearchSplit>> splitsByColumn,
                IndexPathFactory indexPathFactory,
                GlobalIndexFileReader indexFileReader,
                ExecutorService executor);
    }

    private static class RawFullTextIndex implements Closeable {

        private final String column;
        private final int columnIndex;
        private final String indexType;
        private final int fieldId;
        private final long rowRangeStart;
        private final long rowRangeEnd;
        private final GlobalIndexSingleColumnWriter writer;
        private final RawFullTextIndexFileWriter fileWriter;
        private final RoaringNavigableMap64 indexedRows = new RoaringNavigableMap64();

        private RawFullTextIndex(
                String column,
                int columnIndex,
                String indexType,
                int fieldId,
                long rowRangeStart,
                long rowRangeEnd,
                GlobalIndexSingleColumnWriter writer,
                RawFullTextIndexFileWriter fileWriter) {
            this.column = column;
            this.columnIndex = columnIndex;
            this.indexType = indexType;
            this.fieldId = fieldId;
            this.rowRangeStart = rowRangeStart;
            this.rowRangeEnd = rowRangeEnd;
            this.writer = writer;
            this.fileWriter = fileWriter;
        }

        private void write(InternalRow row, long rowId) {
            if (row.isNullAt(columnIndex)) {
                return;
            }
            writer.write(row.getString(columnIndex), rowId - rowRangeStart);
            indexedRows.add(rowId);
        }

        @Nullable
        private IndexFullTextSearchSplit finish() {
            List<ResultEntry> resultEntries = writer.finish();
            if (resultEntries.isEmpty() || indexedRows.isEmpty()) {
                return null;
            }

            List<IndexFileMeta> indexFiles = new ArrayList<>(resultEntries.size());
            for (ResultEntry entry : resultEntries) {
                byte[] bytes = fileWriter.fileBytes(entry.fileName());
                GlobalIndexMeta meta =
                        new GlobalIndexMeta(
                                rowRangeStart, rowRangeEnd, fieldId, null, entry.meta());
                indexFiles.add(
                        new IndexFileMeta(
                                indexType,
                                entry.fileName(),
                                bytes.length,
                                entry.rowCount(),
                                meta,
                                null));
            }
            return new IndexFullTextSearchSplit(column, rowRangeStart, rowRangeEnd, indexFiles);
        }

        private Path path(String fileName) {
            return fileWriter.path(fileName);
        }

        private byte[] fileBytes(String fileName) {
            return fileWriter.fileBytes(fileName);
        }

        private boolean containsFile(String fileName) {
            return fileWriter.containsFile(fileName);
        }

        @Override
        public void close() {
            if (writer instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) writer);
            }
        }
    }

    private static class RawFullTextIndexFileWriter implements GlobalIndexFileWriter {

        private final String column;
        private final String id = UUID.randomUUID().toString();
        private final Map<String, byte[]> files = new HashMap<>();

        private RawFullTextIndexFileWriter(String column) {
            this.column = column;
        }

        @Override
        public String newFileName(String prefix) {
            return "raw-" + column + "-" + prefix + "-" + id + "-" + files.size() + ".index";
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) {
            return new MemoryPositionOutputStream(bytes -> files.put(fileName, bytes));
        }

        private Path path(String fileName) {
            return new Path("memory://full-text-raw/" + fileName);
        }

        private byte[] fileBytes(String fileName) {
            return checkNotNull(files.get(fileName));
        }

        private boolean containsFile(String fileName) {
            return files.containsKey(fileName);
        }
    }

    private static class RawFullTextIndexPathFactory implements IndexPathFactory {

        private final Map<String, RawFullTextIndex> rawIndexes;

        private RawFullTextIndexPathFactory(Map<String, RawFullTextIndex> rawIndexes) {
            this.rawIndexes = rawIndexes;
        }

        @Override
        public Path toPath(String fileName) {
            RawFullTextIndex rawIndex = rawIndex(fileName);
            return rawIndex.path(fileName);
        }

        @Override
        public Path newPath() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isExternalPath() {
            return false;
        }

        private RawFullTextIndex rawIndex(String fileName) {
            for (RawFullTextIndex rawIndex : rawIndexes.values()) {
                if (rawIndex.containsFile(fileName)) {
                    return rawIndex;
                }
            }
            throw new IllegalArgumentException("Unknown raw full-text index file: " + fileName);
        }
    }

    private static class MemoryPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private final FileCommitter committer;

        private MemoryPositionOutputStream(FileCommitter committer) {
            this.committer = committer;
        }

        @Override
        public long getPos() {
            return out.size();
        }

        @Override
        public void write(int b) {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            out.write(b, off, len);
        }

        @Override
        public void flush() {}

        @Override
        public void close() {
            committer.commit(out.toByteArray());
        }
    }

    private static class MemorySeekableInputStream extends SeekableInputStream {

        private final byte[] bytes;
        private int pos;

        private MemorySeekableInputStream(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public void seek(long desired) {
            if (desired < 0 || desired > bytes.length) {
                throw new IllegalArgumentException("Cannot seek to position: " + desired);
            }
            pos = (int) desired;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= bytes.length) {
                return -1;
            }
            int read = Math.min(len, bytes.length - pos);
            System.arraycopy(bytes, pos, b, off, read);
            pos += read;
            return read;
        }

        @Override
        public void close() {}

        @Override
        public int read() {
            if (pos >= bytes.length) {
                return -1;
            }
            return bytes[pos++] & 0xFF;
        }
    }

    private interface FileCommitter {

        void commit(byte[] bytes);
    }
}
