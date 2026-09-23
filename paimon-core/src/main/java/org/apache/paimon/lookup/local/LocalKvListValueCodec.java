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

package org.apache.paimon.lookup.local;

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.List;

/** Codec for individual list deltas and packed initial lists. */
final class LocalKvListValueCodec {

    private static final byte SINGLE_VALUE = 1;
    private static final byte PACKED_VALUES = 2;

    private final DataInputDeserializer input = new DataInputDeserializer();
    private final DataOutputSerializer output = new DataOutputSerializer(128);

    byte[] encodeSingle(byte[] value) {
        byte[] result = new byte[value.length + 1];
        result[0] = SINGLE_VALUE;
        System.arraycopy(value, 0, result, 1, value.length);
        return result;
    }

    byte[] encodeList(List<byte[]> values) throws IOException {
        output.clear();
        output.writeByte(PACKED_VALUES);
        output.writeInt(values.size());
        for (byte[] value : values) {
            output.writeInt(value.length);
            output.write(value);
        }
        return output.getCopyOfBuffer();
    }

    byte[] merge(List<byte[]> storedValues, LocalKvValueCodec valueCodec) throws IOException {
        long valueCount = 0;
        long payloadLength = 0;
        for (byte[] stored : storedValues) {
            int valueOffset = valueCodec.valueOffset(stored, 0, stored.length);
            input.setBuffer(stored, valueOffset, stored.length - valueOffset);
            int type = input.readUnsignedByte();
            if (type == SINGLE_VALUE) {
                valueCount++;
                payloadLength += Integer.BYTES + (long) input.available();
                continue;
            }
            if (type != PACKED_VALUES) {
                throw new IOException("Corrupted local KV list value marker.");
            }

            int size = input.readInt();
            if (size < 0 || size > input.available() / Integer.BYTES) {
                throw new IOException("Corrupted local KV list size: " + size + '.');
            }
            int storedPayloadLength = input.available();
            for (int i = 0; i < size; i++) {
                int elementLength = input.readInt();
                if (elementLength < 0 || elementLength > input.available()) {
                    throw new IOException(
                            "Corrupted local KV list element length: " + elementLength + '.');
                }
                input.skipBytesToRead(elementLength);
            }
            if (input.available() != 0) {
                throw new IOException(
                        "Corrupted local KV list with " + input.available() + " trailing bytes.");
            }
            valueCount += size;
            payloadLength += storedPayloadLength;
        }
        if (valueCount > Integer.MAX_VALUE || payloadLength > Integer.MAX_VALUE - 5) {
            throw new IOException("Merged local KV list value is too large.");
        }

        byte[] packed = new byte[5 + (int) payloadLength];
        packed[0] = PACKED_VALUES;
        writeInt(packed, 1, (int) valueCount);
        int outputOffset = 5;
        for (byte[] stored : storedValues) {
            outputOffset = copyStoredValue(stored, valueCodec, packed, outputOffset);
        }
        return valueCodec.encode(packed);
    }

    <V> void decode(byte[] bytes, int offset, int length, Serializer<V> serializer, List<V> target)
            throws IOException {
        if (length <= 0) {
            throw new IOException("Corrupted empty local KV list value.");
        }
        input.setBuffer(bytes, offset, length);
        int type = input.readUnsignedByte();
        if (type == SINGLE_VALUE) {
            target.add(deserializeElement(input.available(), serializer));
            return;
        }
        if (type != PACKED_VALUES) {
            throw new IOException("Corrupted local KV list value marker.");
        }

        int size = input.readInt();
        if (size < 0 || size > input.available() / Integer.BYTES) {
            throw new IOException(
                    "Corrupted local KV list size: "
                            + size
                            + ", remaining bytes: "
                            + input.available());
        }

        for (int i = 0; i < size; i++) {
            int elementLength = input.readInt();
            if (elementLength < 0 || elementLength > input.available()) {
                throw new IOException(
                        "Corrupted local KV list element length: "
                                + elementLength
                                + ", remaining bytes: "
                                + input.available());
            }
            target.add(deserializeElement(elementLength, serializer));
        }
        if (input.available() != 0) {
            throw new IOException(
                    "Corrupted local KV list with " + input.available() + " trailing bytes.");
        }
    }

    private <V> V deserializeElement(int length, Serializer<V> serializer) throws IOException {
        int start = input.getPosition();
        V value = serializer.deserialize(input);
        int consumed = input.getPosition() - start;
        if (consumed != length) {
            throw new IOException(
                    "Corrupted local KV list element length: expected "
                            + length
                            + " bytes, consumed "
                            + consumed
                            + '.');
        }
        return value;
    }

    private int copyStoredValue(
            byte[] stored, LocalKvValueCodec valueCodec, byte[] target, int targetOffset)
            throws IOException {
        int valueOffset = valueCodec.valueOffset(stored, 0, stored.length);
        input.setBuffer(stored, valueOffset, stored.length - valueOffset);
        int type = input.readUnsignedByte();
        if (type == SINGLE_VALUE) {
            int valueLength = input.available();
            writeInt(target, targetOffset, valueLength);
            targetOffset += Integer.BYTES;
            System.arraycopy(stored, input.getPosition(), target, targetOffset, valueLength);
            return targetOffset + valueLength;
        }
        if (type != PACKED_VALUES) {
            throw new IOException("Corrupted local KV list value marker.");
        }

        input.readInt();
        int payloadLength = input.available();
        System.arraycopy(stored, input.getPosition(), target, targetOffset, payloadLength);
        return targetOffset + payloadLength;
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }
}
