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

package org.apache.paimon.index;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.index.IndexFileMetaSerializer.rowArrayDataToDvMetas;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Deserializer for {@link IndexFileMeta} in commit message version 11. */
public class IndexFileMetaV4Deserializer implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final RowType GLOBAL_INDEX_SCHEMA =
            new RowType(
                    true,
                    Arrays.asList(
                            new DataField(0, "_ROW_RANGE_START", new BigIntType(false)),
                            new DataField(1, "_ROW_RANGE_END", new BigIntType(false)),
                            new DataField(2, "_INDEX_FIELD_ID", new IntType(false)),
                            new DataField(
                                    3, "_EXTRA_FIELD_IDS", DataTypes.ARRAY(new IntType(false))),
                            new DataField(4, "_INDEX_META", DataTypes.BYTES())));

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_INDEX_TYPE", newStringType(false)),
                            new DataField(1, "_FILE_NAME", newStringType(false)),
                            new DataField(2, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(3, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(
                                    4,
                                    "_DELETIONS_VECTORS_RANGES",
                                    new ArrayType(true, DeletionVectorMeta.SCHEMA)),
                            new DataField(5, "_EXTERNAL_PATH", newStringType(true)),
                            new DataField(6, "_GLOBAL_INDEX", GLOBAL_INDEX_SCHEMA)));

    private final InternalRowSerializer rowSerializer;

    public IndexFileMetaV4Deserializer() {
        this.rowSerializer = InternalSerializers.create(SCHEMA);
    }

    private IndexFileMeta fromRow(InternalRow row) {
        GlobalIndexMeta globalIndexMeta = null;
        if (!row.isNullAt(6)) {
            InternalRow globalIndexRow = row.getRow(6, GLOBAL_INDEX_SCHEMA.getFieldCount());
            long rowRangeStart = globalIndexRow.getLong(0);
            long rowRangeEnd = globalIndexRow.getLong(1);
            int indexFieldId = globalIndexRow.getInt(2);
            int[] extraFields =
                    globalIndexRow.isNullAt(3) ? null : globalIndexRow.getArray(3).toIntArray();
            byte[] indexMeta = globalIndexRow.isNullAt(4) ? null : globalIndexRow.getBinary(4);
            globalIndexMeta =
                    new GlobalIndexMeta(
                            rowRangeStart, rowRangeEnd, indexFieldId, extraFields, indexMeta);
        }

        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3),
                row.isNullAt(4) ? null : rowArrayDataToDvMetas(row.getArray(4)),
                row.isNullAt(5) ? null : row.getString(5).toString(),
                globalIndexMeta);
    }

    public List<IndexFileMeta> deserializeList(DataInputView source) throws IOException {
        int size = source.readInt();
        List<IndexFileMeta> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(fromRow(rowSerializer.deserialize(source)));
        }
        return records;
    }
}
