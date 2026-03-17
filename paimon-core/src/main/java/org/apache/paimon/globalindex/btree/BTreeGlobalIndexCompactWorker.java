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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Compact worker for BTree global index.
 *
 * <p>Executes {@link BTreeGlobalIndexCompactTask} produced by {@link
 * BTreeGlobalIndexCompactCoordinator}. This class performs the actual merge of btree index files:
 *
 * <ul>
 *   <li>Read each source file as ordered (key -> localRowIds) stream.
 *   <li>Rebase local row ids from old range start to merged range start.
 *   <li>k-way merge all streams by key, then append to output writers.
 * </ul>
 */
public class BTreeGlobalIndexCompactWorker {

    private final RowType rowType;
    private final Options options;
    private final FileIO fileIO;
    private final IndexPathFactory indexPathFactory;
    private final GlobalIndexFileReadWrite fileReadWrite;

    public BTreeGlobalIndexCompactWorker(
            FileIO fileIO, RowType rowType, Options options, IndexPathFactory indexPathFactory) {
        this.fileIO = fileIO;
        this.rowType = rowType;
        this.options = options;
        this.indexPathFactory = indexPathFactory;
        this.fileReadWrite = new GlobalIndexFileReadWrite(fileIO, indexPathFactory);
    }

    /**
     * Execute a compaction task and return the resulting index manifest entries.
     *
     * @param task the compaction task to execute
     * @return new index manifest entries (ADD kind) for the compacted files
     */
    public List<IndexManifestEntry> execute(BTreeGlobalIndexCompactTask task) throws IOException {
        DataField indexField = rowType.getField(task.indexFieldId());
        BTreeGlobalIndexer globalIndexer = new BTreeGlobalIndexer(indexField, options);

        List<ResultEntry> results =
                compactCore(globalIndexer, task.rangeGroups(), task.mergedRange(), indexField);
        validateOutputMetaNonOverlap(results, indexField);

        List<IndexFileMeta> newIndexFiles =
                toIndexFileMetas(
                        fileIO,
                        indexPathFactory,
                        new CoreOptions(options),
                        task.mergedRange(),
                        task.indexFieldId(),
                        task.indexType(),
                        results);

        List<IndexManifestEntry> mergedEntries = new ArrayList<>(newIndexFiles.size());
        for (IndexFileMeta indexFileMeta : newIndexFiles) {
            mergedEntries.add(
                    new IndexManifestEntry(
                            FileKind.ADD, task.partition(), task.bucket(), indexFileMeta));
        }
        return mergedEntries;
    }

    /** Core compaction: k-way merge all streams by key, rebase row ids, write to output files. */
    private List<ResultEntry> compactCore(
            BTreeGlobalIndexer globalIndexer,
            List<BTreeGlobalIndexCompactTask.RangeGroup> rangeGroups,
            Range mergedRange,
            DataField indexField)
            throws IOException {
        KeySerializer keySerializer = KeySerializer.create(indexField.type());
        Comparator<Object> comparator = keySerializer.createComparator();
        long targetRowsPerFile = options.get(BTreeIndexOptions.BTREE_INDEX_RECORDS_PER_RANGE);
        checkArgument(
                targetRowsPerFile > 0,
                "btree-index.records-per-range must be > 0, but got %s",
                targetRowsPerFile);
        CacheManager cacheManager =
                new CacheManager(
                        options.get(BTreeIndexOptions.BTREE_INDEX_CACHE_SIZE),
                        options.get(BTreeIndexOptions.BTREE_INDEX_HIGH_PRIORITY_POOL_RATIO));

        List<GroupCursor> groupCursors = new ArrayList<>();
        List<ResultEntry> resultEntries = new ArrayList<>();
        WriterContext writerContext = new WriterContext(globalIndexer);
        try {
            for (BTreeGlobalIndexCompactTask.RangeGroup rangeGroup : rangeGroups) {
                groupCursors.add(
                        new GroupCursor(
                                rangeGroup.files(),
                                keySerializer,
                                comparator,
                                cacheManager,
                                mergedRange.from));
            }

            // write null rows first for all source files.
            for (GroupCursor cursor : groupCursors) {
                cursor.scanNullRowIds(writerContext);
            }

            List<GroupEntry> groupEntries = new ArrayList<>(groupCursors.size());
            for (GroupCursor cursor : groupCursors) {
                groupEntries.add(cursor.next());
            }

            LoserTree loserTree = new LoserTree(groupEntries, comparator);
            int winner;
            while ((winner = loserTree.winner()) >= 0) {
                Object key = groupEntries.get(winner).key;
                List<Long> mergedRows = new ArrayList<>();

                while (winner >= 0 && comparator.compare(groupEntries.get(winner).key, key) == 0) {
                    mergedRows.addAll(groupEntries.get(winner).rowIds);
                    groupEntries.set(winner, groupCursors.get(winner).next());
                    loserTree.adjust(winner);
                    winner = loserTree.winner();
                }

                if (writerContext.shouldRotateBeforeKey(targetRowsPerFile, mergedRows.size())) {
                    resultEntries.add(writerContext.finishAndCreateNext());
                }
                for (Long row : mergedRows) {
                    writerContext.writeKey(key, row);
                }
            }
            if (writerContext.hasData()) {
                resultEntries.add(writerContext.finishCurrent());
            }
            return resultEntries;
        } finally {
            for (GroupCursor cursor : groupCursors) {
                cursor.close();
            }
        }
    }

    private void validateOutputMetaNonOverlap(List<ResultEntry> results, DataField indexField) {
        KeySerializer keySerializer = KeySerializer.create(indexField.type());
        Comparator<Object> comparator = keySerializer.createComparator();

        List<BTreeIndexMeta> keyMetas = new ArrayList<>();
        for (ResultEntry result : results) {
            checkArgument(result.meta() != null, "BTree compaction result meta must not be null.");
            BTreeIndexMeta meta = BTreeIndexMeta.deserialize(result.meta());
            if (!meta.onlyNulls()) {
                keyMetas.add(meta);
            }
        }

        keyMetas.sort(
                (m1, m2) ->
                        comparator.compare(
                                keySerializer.deserialize(MemorySlice.wrap(m1.getFirstKey())),
                                keySerializer.deserialize(MemorySlice.wrap(m2.getFirstKey()))));

        for (int i = 1; i < keyMetas.size(); i++) {
            Object prevLast =
                    keySerializer.deserialize(MemorySlice.wrap(keyMetas.get(i - 1).getLastKey()));
            Object currentFirst =
                    keySerializer.deserialize(MemorySlice.wrap(keyMetas.get(i).getFirstKey()));
            checkArgument(
                    comparator.compare(prevLast, currentFirst) < 0,
                    "BTree compaction output key ranges overlap: previousLast=%s, currentFirst=%s",
                    prevLast,
                    currentFirst);
        }
    }

    private GlobalIndexIOMeta toIOMeta(IndexManifestEntry entry) {
        GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
        checkArgument(meta != null, "Global index meta must not be null.");
        return new GlobalIndexIOMeta(
                indexPathFactory.toPath(entry.indexFile()),
                entry.indexFile().fileSize(),
                meta.indexMeta());
    }

    private static Range rowRange(IndexManifestEntry entry) {
        GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
        checkArgument(meta != null, "Global index meta must not be null.");
        return new Range(meta.rowRangeStart(), meta.rowRangeEnd());
    }

    private static class MergeCursor {
        private final BTreeIndexReader reader;
        private final BTreeIndexReader.EntryIterator iterator;
        private final long rowIdDelta;
        @Nullable private BTreeIndexReader.KeyRowIds current;

        private MergeCursor(BTreeIndexReader reader, long rowIdDelta) {
            this.reader = reader;
            this.iterator = reader.entryIterator();
            this.rowIdDelta = rowIdDelta;
            this.current = null;
        }

        private boolean advance() throws IOException {
            if (!iterator.hasNext()) {
                current = null;
                return false;
            }
            current = iterator.next();
            return true;
        }

        private void close() throws IOException {
            reader.close();
        }
    }

    private class GroupCursor {
        private final List<MergeCursor> fileCursors;
        private final PriorityQueue<MergeCursor> heap;
        private final Comparator<Object> comparator;

        private GroupCursor(
                List<IndexManifestEntry> files,
                KeySerializer keySerializer,
                Comparator<Object> comparator,
                CacheManager cacheManager,
                long mergedRangeStart)
                throws IOException {
            this.fileCursors = new ArrayList<>(files.size());
            this.comparator = comparator;
            this.heap = new PriorityQueue<>(Comparator.comparing(o -> o.current.key(), comparator));
            for (IndexManifestEntry file : files) {
                long delta = rowRange(file).from - mergedRangeStart;
                GlobalIndexIOMeta ioMeta = toIOMeta(file);
                BTreeIndexReader reader =
                        new BTreeIndexReader(keySerializer, fileReadWrite, ioMeta, cacheManager);
                MergeCursor cursor = new MergeCursor(reader, delta);
                fileCursors.add(cursor);
                if (cursor.advance()) {
                    heap.add(cursor);
                }
            }
        }

        private void scanNullRowIds(WriterContext writerContext) {
            for (MergeCursor cursor : fileCursors) {
                final long delta = cursor.rowIdDelta;
                cursor.reader.scanNullRowIds(
                        rowId -> {
                            try {
                                writerContext.writeNull(rowId + delta);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }

        @Nullable
        private GroupEntry next() throws IOException {
            if (heap.isEmpty()) {
                return null;
            }
            MergeCursor first = heap.poll();
            Object key = first.current.key();
            List<Long> rows = new ArrayList<>();
            List<MergeCursor> sameKey = new ArrayList<>();
            sameKey.add(first);
            while (!heap.isEmpty() && comparator.compare(heap.peek().current.key(), key) == 0) {
                sameKey.add(heap.poll());
            }
            for (MergeCursor cursor : sameKey) {
                for (long rowId : cursor.current.rowIds()) {
                    rows.add(rowId + cursor.rowIdDelta);
                }
                if (cursor.advance()) {
                    heap.add(cursor);
                }
            }
            return new GroupEntry(key, rows);
        }

        private void close() throws IOException {
            for (MergeCursor cursor : fileCursors) {
                cursor.close();
            }
        }
    }

    private static class GroupEntry {
        private final Object key;
        private final List<Long> rowIds;

        private GroupEntry(Object key, List<Long> rowIds) {
            this.key = key;
            this.rowIds = rowIds;
        }
    }

    /** Loser tree for merging group entries by key. */
    private static class LoserTree {
        private final int k;
        private final int[] tree;
        private final List<GroupEntry> entries;
        private final Comparator<Object> comparator;

        private LoserTree(List<GroupEntry> entries, Comparator<Object> comparator) {
            this.k = entries.size();
            this.entries = entries;
            this.comparator = comparator;
            this.tree = new int[k];
            for (int i = 0; i < k; i++) {
                tree[i] = -1;
            }
            for (int i = 0; i < k; i++) {
                adjust(i);
            }
        }

        private int winner() {
            if (k == 0) {
                return -1;
            }
            int winner = tree[0];
            return winner >= 0 && entries.get(winner) != null ? winner : -1;
        }

        private void adjust(int index) {
            int s = index;
            int t = (s + k) >> 1;
            while (t > 0) {
                if (tree[t] == -1 || greater(s, tree[t])) {
                    int tmp = s;
                    s = tree[t];
                    tree[t] = tmp;
                }
                t >>= 1;
            }
            tree[0] = s;
        }

        private boolean greater(int i, int j) {
            if (i < 0) {
                return j >= 0;
            }
            if (j < 0) {
                return false;
            }
            GroupEntry ei = entries.get(i);
            GroupEntry ej = entries.get(j);
            if (ei == null) {
                return ej != null;
            }
            if (ej == null) {
                return false;
            }
            int cmp = comparator.compare(ei.key, ej.key);
            if (cmp != 0) {
                return cmp > 0;
            }
            return i > j;
        }
    }

    private class WriterContext {
        private final BTreeGlobalIndexer globalIndexer;
        @Nullable private BTreeIndexWriter writer;
        private long writtenRows;
        private boolean wroteNonNullKey;

        private WriterContext(BTreeGlobalIndexer globalIndexer) {
            this.globalIndexer = globalIndexer;
            this.writer = null;
            this.writtenRows = 0;
            this.wroteNonNullKey = false;
        }

        private boolean hasData() {
            return writtenRows > 0;
        }

        private void writeNull(long rowId) throws IOException {
            ensureWriter().write(null, rowId);
            writtenRows++;
        }

        private void writeKey(Object key, long rowId) throws IOException {
            ensureWriter().write(key, rowId);
            writtenRows++;
            wroteNonNullKey = true;
        }

        private boolean shouldRotateBeforeKey(long targetRowsPerFile, long nextKeyRows) {
            return hasData() && wroteNonNullKey && writtenRows + nextKeyRows > targetRowsPerFile;
        }

        private ResultEntry finishAndCreateNext() throws IOException {
            ResultEntry result = finishCurrent();
            writer = null;
            writtenRows = 0;
            wroteNonNullKey = false;
            return result;
        }

        private ResultEntry finishCurrent() {
            checkArgument(writer != null, "Writer should not be null when finishing.");
            List<ResultEntry> entries = writer.finish();
            checkArgument(
                    entries.size() == 1, "Expected one result entry, but got %s", entries.size());
            return entries.get(0);
        }

        private BTreeIndexWriter ensureWriter() throws IOException {
            if (writer != null) {
                return writer;
            }
            writer = globalIndexer.createWriter(fileReadWrite);
            return writer;
        }
    }
}
