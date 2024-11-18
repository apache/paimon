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

package org.apache.paimon.flink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/** A {@link RowData} implements {@link Serializable}. */
public class SerializableRowData implements RowData, Serializable {

    private final TypeSerializer<RowData> serializer;

    private transient RowData row;

    public SerializableRowData(RowData row, TypeSerializer<RowData> serializer) {
        this.row = row;
        this.serializer = serializer;
    }

    private synchronized void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        // This following invocation needs to be synchronized to avoid racing problems when the
        // serializer is reused across multiple subtasks.
        serializer.serialize(row, new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        row = serializer.deserialize(new DataInputViewStreamWrapper(in));
    }

    @Override
    public int getArity() {
        return row.getArity();
    }

    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    @Override
    public void setRowKind(RowKind rowKind) {
        row.setRowKind(rowKind);
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return row.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return row.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return row.getDouble(i);
    }

    @Override
    public StringData getString(int i) {
        return row.getString(i);
    }

    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        return row.getDecimal(i, precision, scale);
    }

    @Override
    public TimestampData getTimestamp(int i, int precision) {
        return row.getTimestamp(i, precision);
    }

    @Override
    public <T> RawValueData<T> getRawValue(int i) {
        return row.getRawValue(i);
    }

    @Override
    public byte[] getBinary(int i) {
        return row.getBinary(i);
    }

    @Override
    public ArrayData getArray(int i) {
        return row.getArray(i);
    }

    @Override
    public MapData getMap(int i) {
        return row.getMap(i);
    }

    @Override
    public RowData getRow(int i, int rowArity) {
        return row.getRow(i, rowArity);
    }
}
