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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.crosspartition.KeyPartOrRow;
import org.apache.paimon.flink.utils.InternalTypeSerializer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

import static org.apache.paimon.crosspartition.KeyPartOrRow.KEY_PART;

/** A {@link InternalTypeSerializer} to serialize KeyPartOrRow with T. */
public class KeyWithRowSerializer<T> extends InternalTypeSerializer<Tuple2<KeyPartOrRow, T>> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<T> keyPartSerializer;
    private final TypeSerializer<T> rowSerializer;

    public KeyWithRowSerializer(
            TypeSerializer<T> keyPartSerializer, TypeSerializer<T> rowSerializer) {
        this.keyPartSerializer = keyPartSerializer;
        this.rowSerializer = rowSerializer;
    }

    @Override
    public TypeSerializer<Tuple2<KeyPartOrRow, T>> duplicate() {
        return new KeyWithRowSerializer<>(keyPartSerializer.duplicate(), rowSerializer.duplicate());
    }

    @Override
    public Tuple2<KeyPartOrRow, T> createInstance() {
        return new Tuple2<>();
    }

    private TypeSerializer<T> serializer(KeyPartOrRow keyPartOrRow) {
        return keyPartOrRow == KEY_PART ? keyPartSerializer : rowSerializer;
    }

    @Override
    public Tuple2<KeyPartOrRow, T> copy(Tuple2<KeyPartOrRow, T> from) {
        return new Tuple2<>(from.f0, serializer(from.f0).copy(from.f1));
    }

    @Override
    public void serialize(Tuple2<KeyPartOrRow, T> record, DataOutputView target)
            throws IOException {
        target.writeByte(record.f0.toByteValue());
        serializer(record.f0).serialize(record.f1, target);
    }

    @Override
    public Tuple2<KeyPartOrRow, T> deserialize(DataInputView source) throws IOException {
        KeyPartOrRow keyPartOrRow = KeyPartOrRow.fromByteValue(source.readByte());
        T row = serializer(keyPartOrRow).deserialize(source);
        return new Tuple2<>(keyPartOrRow, row);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyWithRowSerializer<?> that = (KeyWithRowSerializer<?>) o;
        return Objects.equals(keyPartSerializer, that.keyPartSerializer)
                && Objects.equals(rowSerializer, that.rowSerializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyPartSerializer, rowSerializer);
    }
}
