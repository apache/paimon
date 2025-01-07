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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Serializer for {@link IndexFileMeta} with 0.9 version. */
public class IndexFileMeta09Serializer implements Serializable {

    private static final long serialVersionUID = 1L;

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
                                    new ArrayType(
                                            true,
                                            RowType.of(
                                                    newStringType(false),
                                                    new IntType(false),
                                                    new IntType(false))))));

    protected final InternalRowSerializer rowSerializer;

    public IndexFileMeta09Serializer() {
        this.rowSerializer = InternalSerializers.create(SCHEMA);
    }

    public IndexFileMeta fromRow(InternalRow row) {
        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3),
                row.isNullAt(4) ? null : rowArrayDataToDvMetas(row.getArray(4)));
    }

    public final List<IndexFileMeta> deserializeList(DataInputView source) throws IOException {
        int size = source.readInt();
        List<IndexFileMeta> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(deserialize(source));
        }
        return records;
    }

    public IndexFileMeta deserialize(DataInputView in) throws IOException {
        return fromRow(rowSerializer.deserialize(in));
    }

    public static LinkedHashMap<String, DeletionVectorMeta> rowArrayDataToDvMetas(
            InternalArray arrayData) {
        LinkedHashMap<String, DeletionVectorMeta> dvMetas = new LinkedHashMap<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            InternalRow row = arrayData.getRow(i, 3);
            dvMetas.put(
                    row.getString(0).toString(),
                    new DeletionVectorMeta(
                            row.getString(0).toString(), row.getInt(1), row.getInt(2), null));
        }
        return dvMetas;
    }
}
