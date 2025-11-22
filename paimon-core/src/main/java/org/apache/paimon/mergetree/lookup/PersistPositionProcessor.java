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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.function.Function;

import static org.apache.paimon.utils.VarLengthIntUtils.MAX_VAR_LONG_SIZE;
import static org.apache.paimon.utils.VarLengthIntUtils.decodeLong;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeLong;

/** A {@link PersistProcessor} to return {@link PositionedKeyValue}. */
public class PersistPositionProcessor implements PersistProcessor<PositionedKeyValue> {

    private final Function<InternalRow, byte[]> serializer;
    private final Function<byte[], InternalRow> deserializer;
    private final boolean persistValue;

    public PersistPositionProcessor(
            Function<InternalRow, byte[]> serializer,
            Function<byte[], InternalRow> deserializer,
            boolean persistValue) {
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.persistValue = persistValue;
    }

    @Override
    public boolean withPosition() {
        return true;
    }

    @Override
    public byte[] persistToDisk(KeyValue kv) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] persistToDisk(KeyValue kv, long rowPosition) {
        if (persistValue) {
            byte[] vBytes = serializer.apply(kv.value());
            byte[] bytes = new byte[vBytes.length + 8 + 8 + 1];
            MemorySegment segment = MemorySegment.wrap(bytes);
            segment.put(0, vBytes);
            segment.putLong(bytes.length - 17, rowPosition);
            segment.putLong(bytes.length - 9, kv.sequenceNumber());
            segment.put(bytes.length - 1, kv.valueKind().toByteValue());
            return bytes;
        } else {
            byte[] bytes = new byte[MAX_VAR_LONG_SIZE];
            int len = encodeLong(bytes, rowPosition);
            return Arrays.copyOf(bytes, len);
        }
    }

    @Override
    public PositionedKeyValue readFromDisk(
            InternalRow key, int level, byte[] bytes, String fileName) {
        if (persistValue) {
            InternalRow value = deserializer.apply(bytes);
            MemorySegment segment = MemorySegment.wrap(bytes);
            long rowPosition = segment.getLong(bytes.length - 17);
            long sequenceNumber = segment.getLong(bytes.length - 9);
            RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
            return new PositionedKeyValue(
                    new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level),
                    fileName,
                    rowPosition);
        } else {
            long rowPosition = decodeLong(bytes, 0);
            return new PositionedKeyValue(null, fileName, rowPosition);
        }
    }

    public static Factory<PositionedKeyValue> factory(RowType valueType, boolean persistValue) {
        return new Factory<PositionedKeyValue>() {
            @Override
            public String identifier() {
                return persistValue ? "position-and-value" : "position";
            }

            @Override
            public PersistProcessor<PositionedKeyValue> create(
                    LookupSerializerFactory serializerFactory, @Nullable RowType fileSchema) {
                return new PersistPositionProcessor(
                        serializerFactory.createSerializer(valueType),
                        serializerFactory.createDeserializer(fileSchema, valueType),
                        persistValue);
            }
        };
    }
}
