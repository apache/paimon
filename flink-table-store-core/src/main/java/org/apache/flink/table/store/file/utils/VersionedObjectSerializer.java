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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** A {@link ObjectSerializer} for versioned serialization. */
public abstract class VersionedObjectSerializer<T> extends ObjectSerializer<T> {

    private static final long serialVersionUID = 1L;

    public VersionedObjectSerializer(RowType rowType) {
        super(versionType(rowType));
    }

    public static RowType versionType(RowType rowType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_VERSION", new IntType(false)));
        fields.addAll(rowType.getFields());
        return new RowType(fields);
    }

    /**
     * Gets the version with which this serializer serializes.
     *
     * @return The version of the serialization schema.
     */
    public abstract int getVersion();

    public abstract RowData convertTo(T record);

    public abstract T convertFrom(int version, RowData row);

    @Override
    public final RowData toRow(T record) {
        return new JoinedRowData().replace(GenericRowData.of(getVersion()), convertTo(record));
    }

    @Override
    public final T fromRow(RowData row) {
        return convertFrom(row.getInt(0), new OffsetRowData(row.getArity() - 1, 1).replace(row));
    }
}
