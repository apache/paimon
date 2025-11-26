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

import java.util.function.Function;

/** A {@link PersistProcessor} to return {@link KeyValue}. */
public class PersistValueProcessor implements PersistProcessor<KeyValue> {

    private final Function<InternalRow, byte[]> serializer;
    private final Function<byte[], InternalRow> deserializer;

    public PersistValueProcessor(
            Function<InternalRow, byte[]> serializer, Function<byte[], InternalRow> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public boolean withPosition() {
        return false;
    }

    @Override
    public byte[] persistToDisk(KeyValue kv) {
        byte[] vBytes = serializer.apply(kv.value());
        byte[] bytes = new byte[vBytes.length + 8 + 1];
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.put(0, vBytes);
        segment.putLong(bytes.length - 9, kv.sequenceNumber());
        segment.put(bytes.length - 1, kv.valueKind().toByteValue());
        return bytes;
    }

    @Override
    public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        InternalRow value = deserializer.apply(bytes);
        long sequenceNumber = MemorySegment.wrap(bytes).getLong(bytes.length - 9);
        RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
        return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
    }

    public static Factory<KeyValue> factory(RowType valueType) {
        return new Factory<KeyValue>() {
            @Override
            public String identifier() {
                return "value";
            }

            @Override
            public PersistProcessor<KeyValue> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistValueProcessor(
                        serializerFactory.createSerializer(valueType),
                        serializerFactory.createDeserializer(
                                fileSerVersion, valueType, fileSchema));
            }
        };
    }
}
