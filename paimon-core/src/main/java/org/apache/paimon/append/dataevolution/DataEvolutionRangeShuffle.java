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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.operation.AppendFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BlobDescriptorResolvingRow;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.UriReaderFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * Shuffle rows contain typed sort keys and a binary copy of the visible row with BLOB descriptors.
 */
public class DataEvolutionRangeShuffle implements Serializable {

    private static final long serialVersionUID = 1L;
    private final FileStoreTable table;
    private final RowType readType;
    private final RowType exchangeType;
    private final int[] fields;

    public DataEvolutionRangeShuffle(FileStoreTable table, List<String> sortFields) {
        this.table = rewriteTable(table);
        this.readType = SpecialFields.rowTypeWithRowId(table.rowType());
        fields = Arrays.copyOf(readType.getFieldIndices(sortFields), sortFields.size() + 1);
        fields[sortFields.size()] = readType.getFieldIndex(SpecialFields.ROW_ID.name());
        RowType.Builder builder = RowType.builder().field("partition", DataTypes.BYTES());
        for (int i = 0; i < fields.length; i++) {
            builder.field("key" + i, readType.getTypeAt(fields[i]));
        }
        exchangeType = builder.field("payload", DataTypes.BYTES()).build();
    }

    private static FileStoreTable rewriteTable(FileStoreTable table) {
        Map<String, String> options =
                new HashMap<>(DataEvolutionCompactTask.BLOB_COMPACT_READ_OPTIONS);
        // A replacement must fail if the source cannot be read in full.
        options.put(CoreOptions.SCAN_IGNORE_LOST_FILE.key(), "false");
        options.put(CoreOptions.SCAN_IGNORE_CORRUPT_FILE.key(), "false");
        options.put(CoreOptions.BLOB_WRITE_NULL_ON_MISSING_FILE.key(), "false");
        options.put(CoreOptions.BLOB_WRITE_NULL_ON_FETCH_FAILURE.key(), "false");
        options.put(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), "false");
        options.put(CoreOptions.WRITE_ONLY.key(), "true");
        return table.copyWithoutTimeTravel(options);
    }

    public RowType exchangeType() {
        return exchangeType;
    }

    public List<String> sortColumns() {
        return new ArrayList<>(exchangeType.getFieldNames().subList(0, payloadIndex()));
    }

    public int payloadIndex() {
        return exchangeType.getFieldCount() - 1;
    }

    public RecordReader<InternalRow> read(IndexedSplit split) throws IOException {
        byte[] partition = split.dataSplit().partition().toBytes();
        InternalRow.FieldGetter[] getters = new InternalRow.FieldGetter[fields.length];
        for (int i = 0; i < fields.length; i++) {
            getters[i] = InternalRow.createFieldGetter(readType.getTypeAt(fields[i]), fields[i]);
        }
        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        return ((AppendOnlyFileStore) table.store())
                .newDataEvolutionRead()
                .withReadType(readType)
                .createReader(split)
                .transform(
                        row -> {
                            GenericRow result = new GenericRow(exchangeType.getFieldCount());
                            result.setField(0, partition);
                            for (int i = 0; i < getters.length; i++) {
                                result.setField(i + 1, getters[i].getFieldOrNull(row));
                            }
                            result.setField(
                                    payloadIndex(), serializer.toBinaryRow(row).copy().toBytes());
                            return result;
                        });
    }

    public ShardWriter createWriter(String commitUser) {
        return new ShardWriter(commitUser);
    }

    /**
     * Consecutive rows in one range-shuffle task are ordered by partition, sort fields and old ID.
     */
    public class ShardWriter implements AutoCloseable {
        private final AppendFileStoreWrite storeWrite;
        private final BlobDescriptorResolvingRow resolved =
                new BlobDescriptorResolvingRow(UriReaderFactory.fromFileIO(table.fileIO()), null);
        private final ProjectedRow output = ProjectedRow.from(table.rowType(), readType);
        private final BinaryRow row = new BinaryRow(readType.getFieldCount());
        private final List<CommitMessage> messages = new ArrayList<>();
        private byte[] partitionBytes;
        private BinaryRow partition;
        private RecordWriter<InternalRow> writer;

        private ShardWriter(String commitUser) {
            storeWrite = (AppendFileStoreWrite) table.store().newWrite(commitUser);
            storeWrite.withWriteType(table.rowType());
        }

        public void write(byte[] key, byte[] payload) throws Exception {
            if (!Arrays.equals(partitionBytes, key)) {
                finishPartition();
                partitionBytes = key.clone();
                partition = new BinaryRow(table.partitionKeys().size());
                partition.pointTo(MemorySegment.wrap(partitionBytes), 0, partitionBytes.length);
                writer = storeWrite.createWriter(partition, 0);
            }
            row.pointTo(MemorySegment.wrap(payload), 0, payload.length);
            writer.write(output.replaceRow(resolved.replace(row)));
        }

        private void finishPartition() throws Exception {
            if (writer != null) {
                List<DataFileMeta> files =
                        writer.prepareCommit(false).newFilesIncrement().newFiles();
                messages.add(replacement(partition, Collections.emptyList(), files));
                writer.close();
                writer = null;
            }
        }

        public List<CommitMessage> finish() throws Exception {
            finishPartition();
            return messages;
        }

        @Override
        public void close() throws Exception {
            try {
                if (writer != null) {
                    writer.close();
                }
            } finally {
                storeWrite.close();
            }
        }
    }

    /**
     * Restore range order before row IDs are allocated, including inputs with no surviving rows.
     */
    public static List<CommitMessage> replacements(
            List<CommitMessage> inputs, List<CommitMessage> orderedOutputs) {
        Map<BinaryRow, List<DataFileMeta>> outputs = new LinkedHashMap<>();
        for (CommitMessage input : inputs) {
            checkState(
                    outputs.put(input.partition(), new ArrayList<>()) == null,
                    "Duplicate range sort input for partition %s.",
                    input.partition());
        }
        for (CommitMessage output : orderedOutputs) {
            List<DataFileMeta> files = outputs.get(output.partition());
            checkState(
                    files != null,
                    "Range sort output for unplanned partition %s.",
                    output.partition());
            files.addAll(((CommitMessageImpl) output).compactIncrement().compactAfter());
        }
        List<CommitMessage> messages = new ArrayList<>();
        for (CommitMessage input : inputs) {
            messages.add(
                    replacement(
                            input.partition(),
                            ((CommitMessageImpl) input).compactIncrement().compactBefore(),
                            outputs.get(input.partition())));
        }
        return messages;
    }

    static CommitMessage replacement(
            BinaryRow partition, List<DataFileMeta> before, List<DataFileMeta> after) {
        return new CommitMessageImpl(
                partition,
                0,
                null,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(before, after, Collections.emptyList()));
    }
}
