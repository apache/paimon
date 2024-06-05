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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.stats.BinaryTableStats;
import org.apache.paimon.stats.FieldStatsArraySerializer;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.InternalRowUtils.toStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A legacy version serializer for {@link DataFileMeta}. */
public class DataFileMetaLegacyV2Serializer extends ObjectSerializer<DataFileMeta> {

    private static final long serialVersionUID = 1L;

    public DataFileMetaLegacyV2Serializer() {
        super(schemaForLessThan08());
    }

    private static RowType schemaForLessThan08() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(1, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(2, "_ROW_COUNT", new BigIntType(false)));
        fields.add(new DataField(3, "_MIN_KEY", newBytesType(false)));
        fields.add(new DataField(4, "_MAX_KEY", newBytesType(false)));
        fields.add(new DataField(5, "_KEY_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(6, "_VALUE_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new DataField(9, "_SCHEMA_ID", new BigIntType(false)));
        fields.add(new DataField(10, "_LEVEL", new IntType(false)));
        fields.add(new DataField(11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))));
        fields.add(new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()));
        return new RowType(fields);
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
                BinaryTableStats.fromRow(row.getRow(5, 3)),
                BinaryTableStats.fromRow(row.getRow(6, 3)),
                row.getLong(7),
                row.getLong(8),
                row.getLong(9),
                row.getInt(10),
                fromStringArrayData(row.getArray(11)),
                row.getTimestamp(12, 3),
                null,
                null);
    }
}
