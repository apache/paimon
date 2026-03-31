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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexBuilderUtils;
import org.apache.paimon.globalindex.GlobalIndexFileReadWrite;
import org.apache.paimon.globalindex.GlobalIndexParallelWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.globalindex.GlobalIndexBuilderUtils.toIndexFileMetas;

/**
 * Builder for BTree global index with embedded file metadata (key index + file-meta index).
 *
 * <p>Key index (index type {@link BTreeGlobalIndexerFactory#IDENTIFIER}, i.e. {@code "btree"}):
 * standard BTree index mapping {@code key -> rowIds}. Per-entry fileNames are also tracked so that
 * the file-meta index can be populated.
 *
 * <p>File-meta index (index type {@link #INDEX_TYPE_FILE_META}): a flat SST that maps {@code
 * fileName -> ManifestEntry bytes}. One entry per data file covered by this index shard.
 *
 * <p>Both parts are committed atomically in a single {@link CommitMessage}.
 */
public class BTreeWithFileMetaBuilder implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    public static final String INDEX_TYPE_FILE_META = "btree_file_meta";

    private final FileStoreTable table;
    private final DataField indexField;

    public BTreeWithFileMetaBuilder(FileStoreTable table, DataField indexField) {
        this.table = table;
        this.indexField = indexField;
    }

    public DataField getIndexField() {
        return indexField;
    }

    /**
     * Builds btree key index + btree_file_meta index for one partition range and returns a single
     * {@link CommitMessage} containing both.
     *
     * @param rowRange global row id range covered by this shard
     * @param partition partition key
     * @param split the DataSplit being indexed (used to gather DataFileMeta for btree_file_meta)
     * @param data iterator of (indexField, globalRowId) rows
     */
    public CommitMessage build(
            Range rowRange, BinaryRow partition, DataSplit split, Iterator<InternalRow> data)
            throws IOException {

        // Build firstRowId -> DataFileMeta interval map for fast lookup
        List<DataFileMeta> dataFiles = split.dataFiles();
        // dataFiles are ordered by firstRowId within a split
        long[] firstRowIds = new long[dataFiles.size()];
        for (int i = 0; i < dataFiles.size(); i++) {
            firstRowIds[i] = dataFiles.get(i).nonNullFirstRowId();
        }

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        GlobalIndexFileReadWrite rw =
                new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);

        // ---- Key index writer (standard BTree key→rowIds) ----
        GlobalIndexWriter rawWriter =
                GlobalIndexBuilderUtils.createIndexWriter(
                        table,
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        indexField,
                        table.coreOptions().toConfiguration());
        if (!(rawWriter instanceof GlobalIndexParallelWriter)) {
            throw new RuntimeException(
                    "BTree writer must implement GlobalIndexParallelWriter, found: "
                            + rawWriter.getClass().getName());
        }
        GlobalIndexParallelWriter keyIndexWriter = (GlobalIndexParallelWriter) rawWriter;

        // ---- File-meta index writer (fileName → ManifestEntry bytes) ----
        ManifestEntrySerializer manifestSerializer = new ManifestEntrySerializer();
        BinaryFileMetaWriter binaryFileMetaWriter =
                new BinaryFileMetaWriter(rw, manifestSerializer);

        // Write file-meta index entries (one per DataFileMeta in the split)
        for (DataFileMeta file : dataFiles) {
            ManifestEntry entry =
                    ManifestEntry.create(
                            FileKind.ADD, partition, split.bucket(), split.totalBuckets(), file);
            binaryFileMetaWriter.write(file.fileName(), entry);
        }

        // Write key-index entries
        InternalRow.FieldGetter indexFieldGetter =
                InternalRow.createFieldGetter(indexField.type(), 0);
        while (data.hasNext()) {
            InternalRow row = data.next();
            long globalRowId = row.getLong(1);
            long localRowId = globalRowId - rowRange.from;
            Object key = indexFieldGetter.getFieldOrNull(row);
            keyIndexWriter.write(key, localRowId);
        }

        // Finish both writers and collect ResultEntries
        List<ResultEntry> keyIndexEntries = keyIndexWriter.finish();
        List<ResultEntry> binaryFileMetaEntries = binaryFileMetaWriter.finish();

        // Build IndexFileMetas for btree key index
        List<IndexFileMeta> keyIndexMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        indexPathFactory,
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        keyIndexEntries);

        // Build IndexFileMetas for btree_file_meta (rowRange and fieldId are same, to allow
        // scanning)
        List<IndexFileMeta> binaryFileMetaIndexMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        indexPathFactory,
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        INDEX_TYPE_FILE_META,
                        binaryFileMetaEntries);

        List<IndexFileMeta> allMetas = new ArrayList<>();
        allMetas.addAll(keyIndexMetas);
        allMetas.addAll(binaryFileMetaIndexMetas);

        // Single CommitMessage → atomic commit of btree key index + btree_file_meta
        DataIncrement dataIncrement = DataIncrement.indexIncrement(allMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    /**
     * Flush overload compatible with the existing {@link BTreeGlobalIndexBuilder} API, where we
     * have already-written btree key index result entries plus an explicit list of ManifestEntry
     * rows for btree_file_meta.
     */
    public CommitMessage flushIndex(
            Range rowRange,
            List<ResultEntry> keyIndexResultEntries,
            List<ResultEntry> binaryFileMetaResultEntries,
            BinaryRow partition)
            throws IOException {

        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();

        List<IndexFileMeta> keyIndexMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        indexPathFactory,
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        BTreeGlobalIndexerFactory.IDENTIFIER,
                        keyIndexResultEntries);

        List<IndexFileMeta> binaryFileMetaIndexMetas =
                toIndexFileMetas(
                        table.fileIO(),
                        indexPathFactory,
                        table.coreOptions(),
                        rowRange,
                        indexField.id(),
                        INDEX_TYPE_FILE_META,
                        binaryFileMetaResultEntries);

        List<IndexFileMeta> allMetas = new ArrayList<>();
        allMetas.addAll(keyIndexMetas);
        allMetas.addAll(binaryFileMetaIndexMetas);

        DataIncrement dataIncrement = DataIncrement.indexIncrement(allMetas);
        return new CommitMessageImpl(
                partition, 0, null, dataIncrement, CompactIncrement.emptyIncrement());
    }

    /**
     * Creates a new binary file-meta index writer that accumulates {@code fileName ->
     * ManifestEntry} mappings. Call {@link BinaryFileMetaWriter#finish()} to get the {@link
     * ResultEntry} list.
     */
    public BinaryFileMetaWriter createBinaryFileMetaWriter() throws IOException {
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        GlobalIndexFileReadWrite rw =
                new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);
        return new BinaryFileMetaWriter(rw, new ManifestEntrySerializer());
    }

    /**
     * Resolves which {@link DataFileMeta} a global row id belongs to by binary search over
     * pre-computed {@code firstRowIds} array.
     */
    static int findFileIndex(long[] firstRowIds, long globalRowId) {
        int lo = 0;
        int hi = firstRowIds.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (firstRowIds[mid] <= globalRowId) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }
}
