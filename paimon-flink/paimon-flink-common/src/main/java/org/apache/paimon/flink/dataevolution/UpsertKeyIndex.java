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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.sort.db.SimpleLsmKvDb;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.VarLengthIntUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/**
 * Manages a business-key-to-{@code _ROW_ID} index for data evolution streaming upsert. Uses {@link
 * SimpleLsmKvDb} for the underlying storage, following the same pattern as {@code
 * ClusteringKeyIndex}.
 */
public class UpsertKeyIndex implements Closeable {

    private final SimpleLsmKvDb kvDb;
    private final RowType upsertKeyType;
    private final RowCompactedSerializer keySerializer;
    private final FieldGetter[] keyFieldGetters;
    private final Map<Long, List<DataFileMeta>> firstIdToFiles;

    private FirstRowIdLookup firstRowIdLookup;
    private InnerTableRead cachedTableRead;

    public UpsertKeyIndex(File dbDir, RowType upsertKeyType) {
        this.upsertKeyType = upsertKeyType;
        this.keySerializer = new RowCompactedSerializer(upsertKeyType);
        this.kvDb =
                SimpleLsmKvDb.builder(dbDir)
                        .keyComparator(keySerializer.createSliceComparator())
                        .build();
        this.firstIdToFiles = new HashMap<>();

        int keyFieldCount = upsertKeyType.getFieldCount();
        this.keyFieldGetters = new FieldGetter[keyFieldCount];
        for (int i = 0; i < keyFieldCount; i++) {
            keyFieldGetters[i] = InternalRow.createFieldGetter(upsertKeyType.getTypeAt(i), i);
        }
    }

    /**
     * Bootstrap the index from existing data files for a given partition. Scans all files, reads
     * (upsert_key, _ROW_ID) pairs, sorts externally, and bulk-loads into the KvDb.
     */
    public void bootstrap(
            FileStoreTable table,
            BinaryRow partition,
            long snapshotId,
            IOManager ioManager,
            long sortBufferSize,
            int pageSize,
            int maxNumFileHandles,
            CompressOptions compression)
            throws Exception {
        List<ManifestEntry> entries = scanPartitionFiles(table, partition, snapshotId);
        buildFileMetadata(entries);

        if (entries.isEmpty()) {
            return;
        }

        int keyFieldCount = upsertKeyType.getFieldCount();

        List<DataField> combinedFields = new ArrayList<>();
        for (int i = 0; i < keyFieldCount; i++) {
            DataField kf = upsertKeyType.getFields().get(i);
            combinedFields.add(new DataField(i, kf.name(), kf.type()));
        }
        combinedFields.add(
                new DataField(keyFieldCount, "_value_bytes", new VarBinaryType(Integer.MAX_VALUE)));
        RowType combinedType = new RowType(combinedFields);

        int[] sortFields = IntStream.range(0, keyFieldCount).toArray();
        BinaryExternalSortBuffer sortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        combinedType,
                        sortFields,
                        sortBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        compression,
                        MemorySize.MAX_VALUE);

        try {
            InnerTableRead tableRead = getOrCreateTableRead(table);

            for (Map.Entry<Long, List<DataFileMeta>> fileGroup : firstIdToFiles.entrySet()) {
                long firstRowId = fileGroup.getKey();
                List<DataFileMeta> files = fileGroup.getValue();
                DataSplit split =
                        DataSplit.builder()
                                .withPartition(partition)
                                .withBucket(0)
                                .withDataFiles(files)
                                .withBucketPath(
                                        table.store()
                                                .pathFactory()
                                                .bucketPath(partition, 0)
                                                .toString())
                                .rawConvertible(false)
                                .build();

                long offset = 0;
                try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                    RecordReader.RecordIterator<InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            long rowId = firstRowId + offset;
                            byte[] value = encodeRowId(rowId);

                            GenericRow combinedRow = new GenericRow(combinedType.getFieldCount());
                            for (int i = 0; i < keyFieldCount; i++) {
                                combinedRow.setField(i, keyFieldGetters[i].getFieldOrNull(row));
                            }
                            combinedRow.setField(keyFieldCount, value);
                            sortBuffer.write(combinedRow);
                            offset++;
                        }
                        batch.releaseBatch();
                    }
                }
            }

            MutableObjectIterator<BinaryRow> sortedIterator = sortBuffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(combinedType.getFieldCount());
            FieldGetter valueGetter =
                    InternalRow.createFieldGetter(
                            new VarBinaryType(Integer.MAX_VALUE), keyFieldCount);

            Iterator<Map.Entry<byte[], byte[]>> entryIterator =
                    new Iterator<Map.Entry<byte[], byte[]>>() {
                        private BinaryRow current = binaryRow;
                        private boolean hasNext;

                        {
                            advance();
                        }

                        private void advance() {
                            try {
                                current = sortedIterator.next(current);
                                hasNext = current != null;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public boolean hasNext() {
                            return hasNext;
                        }

                        @Override
                        public Map.Entry<byte[], byte[]> next() {
                            byte[] key = keySerializer.serializeToBytes(current);
                            byte[] value = (byte[]) valueGetter.getFieldOrNull(current);
                            advance();
                            return new AbstractMap.SimpleImmutableEntry<>(key, value);
                        }
                    };

            kvDb.bulkLoad(entryIterator);
        } finally {
            sortBuffer.clear();
        }
    }

    @Nullable
    public Long lookupRowId(byte[] keyBytes) throws IOException {
        byte[] value = kvDb.get(keyBytes);
        if (value == null) {
            return null;
        }
        return VarLengthIntUtils.decodeLong(value, 0);
    }

    public long lookupFirstRowId(long rowId) {
        return firstRowIdLookup.lookup(rowId);
    }

    /**
     * Incrementally sync the index from new snapshots. Scans the latest snapshot for newly added
     * files (those whose firstRowId is not yet tracked in {@link #firstIdToFiles}), reads their
     * (key, _ROW_ID) pairs, and inserts them into the kvDb.
     */
    public void incrementalSync(
            FileStoreTable table, BinaryRow partition, long oldSnapshotId, long newSnapshotId)
            throws Exception {
        List<ManifestEntry> entries = scanPartitionFiles(table, partition, newSnapshotId);

        Map<Long, List<DataFileMeta>> newFirstIdToFiles = new HashMap<>();
        for (ManifestEntry entry : entries) {
            long firstRowId = entry.file().nonNullFirstRowId();
            newFirstIdToFiles.computeIfAbsent(firstRowId, k -> new ArrayList<>()).add(entry.file());
        }

        int keyFieldCount = upsertKeyType.getFieldCount();
        InnerTableRead tableRead = getOrCreateTableRead(table);

        for (Map.Entry<Long, List<DataFileMeta>> fileGroup : newFirstIdToFiles.entrySet()) {
            long firstRowId = fileGroup.getKey();
            if (firstIdToFiles.containsKey(firstRowId)) {
                continue;
            }

            List<DataFileMeta> files = fileGroup.getValue();
            DataSplit split =
                    DataSplit.builder()
                            .withPartition(partition)
                            .withBucket(0)
                            .withDataFiles(files)
                            .withBucketPath(
                                    table.store().pathFactory().bucketPath(partition, 0).toString())
                            .rawConvertible(false)
                            .build();

            long offset = 0;
            try (RecordReader<InternalRow> reader = tableRead.createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        long rowId = firstRowId + offset;
                        GenericRow keyRow = new GenericRow(keyFieldCount);
                        for (int i = 0; i < keyFieldCount; i++) {
                            keyRow.setField(i, keyFieldGetters[i].getFieldOrNull(row));
                        }
                        byte[] key = keySerializer.serializeToBytes(keyRow);
                        kvDb.put(key, encodeRowId(rowId));
                        offset++;
                    }
                    batch.releaseBatch();
                }
            }
        }

        buildFileMetadata(entries);
    }

    @Override
    public void close() throws IOException {
        kvDb.close();
        cachedTableRead = null;
    }

    private InnerTableRead getOrCreateTableRead(FileStoreTable table) {
        if (cachedTableRead == null) {
            List<String> upsertKeyNames =
                    upsertKeyType.getFields().stream()
                            .map(DataField::name)
                            .collect(Collectors.toList());
            RowType readType = buildReadType(table.rowType(), upsertKeyNames);
            cachedTableRead = table.newRead().withReadType(readType);
        }
        return cachedTableRead;
    }

    private void buildFileMetadata(List<ManifestEntry> entries) {
        TreeSet<Long> rowIdSet = new TreeSet<>();
        firstIdToFiles.clear();
        for (ManifestEntry entry : entries) {
            DataFileMeta fileMeta = entry.file();
            long firstRowId = fileMeta.nonNullFirstRowId();
            rowIdSet.add(firstRowId);
            firstIdToFiles.computeIfAbsent(firstRowId, k -> new ArrayList<>()).add(fileMeta);
        }
        this.firstRowIdLookup = new FirstRowIdLookup(new ArrayList<>(rowIdSet));
    }

    private static RowType buildReadType(RowType tableRowType, List<String> upsertKeyNames) {
        RowType withRowId = SpecialFields.rowTypeWithRowId(tableRowType);
        List<String> projection = new ArrayList<>(upsertKeyNames);
        projection.add(SpecialFields.ROW_ID.name());
        return withRowId.project(projection);
    }

    private static byte[] encodeRowId(long rowId) {
        byte[] buf = new byte[10];
        int len = VarLengthIntUtils.encodeLong(buf, rowId);
        byte[] result = new byte[len];
        System.arraycopy(buf, 0, result, 0, len);
        return result;
    }

    private static List<ManifestEntry> scanPartitionFiles(
            FileStoreTable table, BinaryRow partition, long snapshotId) {
        List<ManifestEntry> allEntries =
                table.store()
                        .newScan()
                        .withManifestEntryFilter(
                                entry ->
                                        entry.file().firstRowId() != null
                                                && !isBlobFile(entry.file().fileName())
                                                && !isVectorStoreFile(entry.file().fileName()))
                        .withSnapshot(snapshotId)
                        .plan()
                        .files();
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : allEntries) {
            if (entry.partition().equals(partition)) {
                result.add(entry);
            }
        }
        return result;
    }
}
