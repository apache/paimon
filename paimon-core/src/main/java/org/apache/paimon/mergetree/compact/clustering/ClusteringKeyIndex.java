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

package org.apache.paimon.mergetree.compact.clustering;

import org.apache.paimon.KeyValue;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.lookup.sort.db.SimpleLsmKvDb;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/**
 * Manages the primary key index for clustering compaction. Maps each primary key to its file
 * location (fileId + row position) using a {@link SimpleLsmKvDb}.
 */
public class ClusteringKeyIndex implements Closeable {

    private final RowType keyType;
    private final IOManager ioManager;
    private final KeyValueFileReaderFactory keyReaderFactory;
    private final BucketedDvMaintainer dvMaintainer;
    private final SimpleLsmKvDb kvDb;
    private final ClusteringFiles fileLevels;
    private final boolean firstRow;
    private final long sortSpillBufferSize;
    private final int pageSize;
    private final int maxNumFileHandles;
    private final CompressOptions compression;

    public ClusteringKeyIndex(
            RowType keyType,
            IOManager ioManager,
            KeyValueFileReaderFactory keyReaderFactory,
            BucketedDvMaintainer dvMaintainer,
            SimpleLsmKvDb kvDb,
            ClusteringFiles fileLevels,
            boolean firstRow,
            long sortSpillBufferSize,
            int pageSize,
            int maxNumFileHandles,
            CompressOptions compression) {
        this.keyType = keyType;
        this.ioManager = ioManager;
        this.keyReaderFactory = keyReaderFactory;
        this.dvMaintainer = dvMaintainer;
        this.kvDb = kvDb;
        this.fileLevels = fileLevels;
        this.firstRow = firstRow;
        this.sortSpillBufferSize = sortSpillBufferSize;
        this.pageSize = pageSize;
        this.maxNumFileHandles = maxNumFileHandles;
        this.compression = compression;
    }

    /** Bootstrap the key index from existing sorted files using external sort + bulk load. */
    public void bootstrap(List<DataFileMeta> restoreFiles) {
        List<DataField> combinedFields = new ArrayList<>();
        List<DataField> keyFields = keyType.getFields();
        for (int i = 0; i < keyFields.size(); i++) {
            DataField kf = keyFields.get(i);
            combinedFields.add(new DataField(i, kf.name(), kf.type()));
        }
        int valueFieldIndex = keyFields.size();
        combinedFields.add(
                new DataField(
                        valueFieldIndex, "_value_bytes", new VarBinaryType(Integer.MAX_VALUE)));
        RowType combinedType = new RowType(combinedFields);

        int[] sortFields = IntStream.range(0, keyType.getFieldCount()).toArray();
        BinaryExternalSortBuffer sortBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        combinedType,
                        sortFields,
                        sortSpillBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        compression,
                        MemorySize.MAX_VALUE,
                        false);

        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        InternalRow.FieldGetter[] keyFieldGetters =
                new InternalRow.FieldGetter[keyType.getFieldCount()];
        for (int i = 0; i < keyType.getFieldCount(); i++) {
            keyFieldGetters[i] = InternalRow.createFieldGetter(keyType.getTypeAt(i), i);
        }
        try {
            for (DataFileMeta file : restoreFiles) {
                if (file.level() == 0) {
                    continue;
                }
                int fileId = fileLevels.getFileIdByName(file.fileName());
                try (RecordReader<KeyValue> reader = keyReaderFactory.createRecordReader(file)) {
                    FileRecordIterator<KeyValue> batch;
                    while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                        KeyValue kv;
                        while ((kv = batch.next()) != null) {
                            int position = (int) batch.returnedPosition();
                            ByteArrayOutputStream valueOut = new ByteArrayOutputStream(8);
                            encodeInt(valueOut, fileId);
                            encodeInt(valueOut, position);
                            byte[] valueBytes = valueOut.toByteArray();

                            GenericRow combinedRow = new GenericRow(combinedType.getFieldCount());
                            for (int i = 0; i < keyType.getFieldCount(); i++) {
                                combinedRow.setField(
                                        i, keyFieldGetters[i].getFieldOrNull(kv.key()));
                            }
                            combinedRow.setField(valueFieldIndex, valueBytes);
                            sortBuffer.write(combinedRow);
                        }
                        batch.releaseBatch();
                    }
                }
            }

            MutableObjectIterator<BinaryRow> sortedIterator = sortBuffer.sortedIterator();
            BinaryRow binaryRow = new BinaryRow(combinedType.getFieldCount());
            InternalRow.FieldGetter valueGetter =
                    InternalRow.createFieldGetter(
                            new VarBinaryType(Integer.MAX_VALUE), valueFieldIndex);

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
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            sortBuffer.clear();
        }
    }

    /**
     * Check a key against the index during sort-and-rewrite writing.
     *
     * <p>For FIRST_ROW mode: if key exists pointing to a non-original file, return false (skip
     * writing this record — it's a duplicate).
     *
     * <p>For DEDUPLICATE mode: if key exists pointing to a non-original file, mark the old position
     * in deletion vectors, return true (write the new record).
     *
     * @param keyBytes serialized key bytes
     * @return true if the record should be written, false to skip (FIRST_ROW dedup)
     */
    public boolean checkKey(byte[] keyBytes) throws Exception {
        byte[] oldValue = kvDb.get(keyBytes);
        if (oldValue != null) {
            ByteArrayInputStream valueIn = new ByteArrayInputStream(oldValue);
            int oldFileId = decodeInt(valueIn);
            int oldPosition = decodeInt(valueIn);
            DataFileMeta oldFile = fileLevels.getFileById(oldFileId);
            if (oldFile != null) {
                if (firstRow) {
                    return false;
                } else {
                    checkNotNull(dvMaintainer, "DvMaintainer cannot be null for DEDUPLICATE mode.");
                    dvMaintainer.notifyNewDeletion(oldFile.fileName(), oldPosition);
                }
            }
        }
        return true;
    }

    /** Delete key index entries for the given file (only if they still point to it). */
    public void deleteIndex(DataFileMeta file) throws Exception {
        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        int fileId = fileLevels.getFileIdByName(file.fileName());
        try (CloseableIterator<InternalRow> iterator = readKeyIterator(file)) {
            while (iterator.hasNext()) {
                byte[] key = keySerializer.serializeToBytes(iterator.next());
                byte[] value = kvDb.get(key);
                if (value != null) {
                    int storedFileId = decodeInt(new ByteArrayInputStream(value));
                    if (storedFileId == fileId) {
                        kvDb.delete(key);
                    }
                }
            }
        }
    }

    /** Rebuild key index entries for a newly written file. */
    public void rebuildIndex(DataFileMeta newFile) throws Exception {
        RowCompactedSerializer keySerializer = new RowCompactedSerializer(keyType);
        int fileId = fileLevels.getFileIdByName(newFile.fileName());
        int position = 0;
        try (CloseableIterator<InternalRow> keyIterator = readKeyIterator(newFile)) {
            while (keyIterator.hasNext()) {
                byte[] key = keySerializer.serializeToBytes(keyIterator.next());
                ByteArrayOutputStream value = new ByteArrayOutputStream(8);
                encodeInt(value, fileId);
                encodeInt(value, position);
                kvDb.put(key, value.toByteArray());
                position++;
            }
        }
    }

    private CloseableIterator<InternalRow> readKeyIterator(DataFileMeta file) throws IOException {
        //noinspection resource
        return keyReaderFactory
                .createRecordReader(file)
                .transform(KeyValue::key)
                .toCloseableIterator();
    }

    @Override
    public void close() throws IOException {
        kvDb.close();
    }
}
