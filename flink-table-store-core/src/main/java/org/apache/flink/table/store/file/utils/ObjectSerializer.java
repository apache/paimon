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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

/** A serializer to serialize object by {@link RowDataSerializer}. */
public abstract class ObjectSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final RowDataSerializer rowSerializer;

    public ObjectSerializer(RowType rowType) {
        this.rowSerializer = InternalSerializers.create(rowType);
    }

    /** Get the number of fields. */
    public int numFields() {
        return rowSerializer.getArity();
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

    /** Convert a {@link T} to {@link RowData}. */
    public abstract RowData toRow(T record);

    /** Convert a {@link RowData} to {@link T}. */
    public abstract T fromRow(RowData rowData);
}
