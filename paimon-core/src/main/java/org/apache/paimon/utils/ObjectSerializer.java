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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** A serializer to serialize object by {@link InternalRowSerializer}. */
public abstract class ObjectSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final InternalRowSerializer rowSerializer;

    public ObjectSerializer(RowType rowType) {
        this.rowSerializer = InternalSerializers.create(rowType);
    }

    /** Get the number of fields. */
    public int numFields() {
        return rowSerializer.getArity();
    }

    public DataType[] fieldTypes() {
        return rowSerializer.fieldTypes();
    }

    /**
     * Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically
     *     raised by the output view, which may have an underlying I/O channel to which it
     *     delegates.
     */
    public final void serialize(T record, DataOutputView target) throws IOException {
        rowSerializer.serialize(toRow(record), target);
    }

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *     Typically raised by the input view, which may have an underlying I/O channel from which
     *     it reads.
     */
    public final T deserialize(DataInputView source) throws IOException {
        return fromRow(rowSerializer.deserialize(source));
    }

    /** Serializes the given record list to the given target output view. */
    public final void serializeList(List<T> records, DataOutputView target) throws IOException {
        target.writeInt(records.size());
        for (T t : records) {
            serialize(t, target);
        }
    }

    public final byte[] serializeList(List<T> records) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);
        serializeList(records, view);
        return baos.toByteArray();
    }

    /** De-serializes a record list from the given source input view. */
    public final List<T> deserializeList(DataInputView source) throws IOException {
        int size = source.readInt();
        List<T> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(deserialize(source));
        }
        return records;
    }

    public final List<T> deserializeList(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bais);
        return deserializeList(view);
    }

    public byte[] serializeToBytes(T record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(record, view);
        return out.toByteArray();
    }

    public T deserializeFromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        return deserialize(view);
    }

    /** Convert a {@link T} to {@link InternalRow}. */
    public abstract InternalRow toRow(T record);

    /** Convert a {@link InternalRow} to {@link T}. */
    public abstract T fromRow(InternalRow rowData);
}
