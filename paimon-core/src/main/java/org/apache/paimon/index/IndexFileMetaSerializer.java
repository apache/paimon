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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.VersionedObjectSerializer;

import java.util.Collection;
import java.util.LinkedHashMap;

/** A {@link VersionedObjectSerializer} for {@link IndexFileMeta}. */
public class IndexFileMetaSerializer extends ObjectSerializer<IndexFileMeta> {

    public IndexFileMetaSerializer() {
        super(IndexFileMeta.SCHEMA);
    }

    @Override
    public InternalRow toRow(IndexFileMeta record) {
        return GenericRow.of(
                BinaryString.fromString(record.indexType()),
                BinaryString.fromString(record.fileName()),
                record.fileSize(),
                record.rowCount(),
                record.deletionVectorMetas() == null
                        ? null
                        : dvMetasToRowArrayData(record.deletionVectorMetas().values()));
    }

    @Override
    public IndexFileMeta fromRow(InternalRow row) {
        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3),
                row.isNullAt(4) ? null : rowArrayDataToDvMetas(row.getArray(4)));
    }

    public static InternalArray dvMetasToRowArrayData(Collection<DeletionVectorMeta> dvMetas) {
        return new GenericArray(
                dvMetas.stream()
                        .map(
                                dvMeta ->
                                        GenericRow.of(
                                                BinaryString.fromString(dvMeta.dataFileName()),
                                                dvMeta.offset(),
                                                dvMeta.length(),
                                                dvMeta.cardinality()))
                        .toArray(GenericRow[]::new));
    }

    public static LinkedHashMap<String, DeletionVectorMeta> rowArrayDataToDvMetas(
            InternalArray arrayData) {
        LinkedHashMap<String, DeletionVectorMeta> dvMetas = new LinkedHashMap<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            InternalRow row = arrayData.getRow(i, DeletionVectorMeta.SCHEMA.getFieldCount());
            dvMetas.put(
                    row.getString(0).toString(),
                    new DeletionVectorMeta(
                            row.getString(0).toString(),
                            row.getInt(1),
                            row.getInt(2),
                            row.isNullAt(3) ? null : row.getLong(3)));
        }
        return dvMetas;
    }
}
