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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.InternalRowUtils.toStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A legacy version serializer for {@link CommitMessage}. */
public class CommitMessageLegacyV2Serializer {

    private DataFileMetaLegacyV2Serializer dataFileSerializer;
    private IndexFileMetaLegacyV2Serializer indexEntrySerializer;

    public List<CommitMessage> deserializeList(DataInputView view) throws IOException {
        int length = view.readInt();
        List<CommitMessage> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(deserialize(view));
        }
        return list;
    }

    public CommitMessage deserialize(DataInputView view) throws IOException {
        if (dataFileSerializer == null) {
            dataFileSerializer = new DataFileMetaLegacyV2Serializer();
            indexEntrySerializer = new IndexFileMetaLegacyV2Serializer();
        }
        return new CommitMessageImpl(
                deserializeBinaryRow(view),
                view.readInt(),
                new DataIncrement(
                        dataFileSerializer.deserializeList(view),
                        Collections.emptyList(),
                        dataFileSerializer.deserializeList(view)),
                new CompactIncrement(
                        dataFileSerializer.deserializeList(view),
                        dataFileSerializer.deserializeList(view),
                        dataFileSerializer.deserializeList(view)),
                new IndexIncrement(indexEntrySerializer.deserializeList(view)));
    }

    private static RowType legacyDataFileSchema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_ROW_COUNT", new BigIntType(false)));
        fields.add(new DataField(3, "_MIN_KEY", newBytesType(false)));
        fields.add(new DataField(4, "_MAX_KEY", newBytesType(false)));
        fields.add(new DataField(5, "_KEY_STATS", SimpleStatsConverter.schema()));
        fields.add(new DataField(6, "_VALUE_STATS", SimpleStatsConverter.schema()));
        fields.add(new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(9, "_SCHEMA_ID", new BigIntType(false)));
        fields.add(new DataField(10, "_LEVEL", new IntType(false)));
        fields.add(new DataField(11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))));
        fields.add(new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()));
        return new RowType(fields);
    }

    private static RowType legacyIndexFileSchema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_INDEX_TYPE", newStringType(false)));
        fields.add(new DataField(1, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(2, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(3, "_ROW_COUNT", new BigIntType(false)));
        return new RowType(fields);
    }

    /** A legacy version serializer for {@link DataFileMeta}. */
    private static class DataFileMetaLegacyV2Serializer extends ObjectSerializer<DataFileMeta> {

        private static final long serialVersionUID = 1L;

        public DataFileMetaLegacyV2Serializer() {
            super(legacyDataFileSchema());
        }

        @Override
        public InternalRow toRow(DataFileMeta meta) {
            return GenericRow.of(
                    BinaryString.fromString(meta.fileName()),
                    meta.fileSize(),
                    meta.rowCount(),
                    serializeBinaryRow(meta.minKey()),
                    serializeBinaryRow(meta.maxKey()),
                    meta.keyStats().toRow(),
                    meta.valueStats().toRow(),
                    meta.minSequenceNumber(),
                    meta.maxSequenceNumber(),
                    meta.schemaId(),
                    meta.level(),
                    toStringArrayData(meta.extraFiles()),
                    meta.creationTime());
        }

        @Override
        public DataFileMeta fromRow(InternalRow row) {
            return new DataFileMeta(
                    row.getString(0).toString(),
                    row.getLong(1),
                    row.getLong(2),
                    deserializeBinaryRow(row.getBinary(3)),
                    deserializeBinaryRow(row.getBinary(4)),
                    SimpleStats.fromRow(row.getRow(5, 3)),
                    SimpleStats.fromRow(row.getRow(6, 3)),
                    row.getLong(7),
                    row.getLong(8),
                    row.getLong(9),
                    row.getInt(10),
                    fromStringArrayData(row.getArray(11)),
                    row.getTimestamp(12, 3),
                    null,
                    null,
                    null);
        }
    }

    /** A legacy version serializer for {@link IndexFileMeta}. */
    private static class IndexFileMetaLegacyV2Serializer extends ObjectSerializer<IndexFileMeta> {

        public IndexFileMetaLegacyV2Serializer() {
            super(legacyIndexFileSchema());
        }

        @Override
        public InternalRow toRow(IndexFileMeta record) {
            return GenericRow.of(
                    BinaryString.fromString(record.indexType()),
                    BinaryString.fromString(record.fileName()),
                    record.fileSize(),
                    record.rowCount());
        }

        @Override
        public IndexFileMeta fromRow(InternalRow row) {
            return new IndexFileMeta(
                    row.getString(0).toString(),
                    row.getString(1).toString(),
                    row.getLong(2),
                    row.getLong(3),
                    null);
        }
    }
}
