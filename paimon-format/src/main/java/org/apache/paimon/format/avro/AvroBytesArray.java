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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.utils.IntArrayList;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** An {@link InternalArray} whose elements are stored as Avro binary payloads. */
public class AvroBytesArray implements InternalArray {

    // stores original element bytes from avro files
    private final byte[] bytes;
    // total bytes length
    private final int lengthInBytes;
    // offset of each element payload
    private final IntArrayList off;
    // length of each element payload
    private final IntArrayList len;
    // reader to decode element payloads lazily
    private final FieldReader elementReader;

    // decoded array, lazily initialized when elements are accessed
    private GenericArray decodedArray;

    private AvroBytesArray(
            byte[] bytes,
            int lengthInBytes,
            IntArrayList off,
            IntArrayList len,
            FieldReader elementReader) {
        this.bytes = bytes;
        this.lengthInBytes = lengthInBytes;
        this.off = off;
        this.len = len;
        this.elementReader = elementReader;
    }

    @Nullable
    public static AvroBytesArray create(
            BinaryDecoder decoder, FieldReader elementReader, Schema elementType)
            throws IOException {
        RawFieldReader rawFieldReader = createRawFieldReader(elementType);
        if (rawFieldReader == null) {
            return null;
        }

        BytesBuilder builder = new BytesBuilder();
        IntArrayList off = new IntArrayList(16);
        IntArrayList len = new IntArrayList(16);

        long chunkLength = decoder.readArrayStart();
        while (chunkLength > 0) {
            for (int i = 0; i < chunkLength; i++) {
                int offset = builder.lengthInBytes;
                rawFieldReader.read(decoder, builder);
                off.add(offset);
                len.add(builder.lengthInBytes - offset);
            }
            chunkLength = decoder.arrayNext();
        }

        return new AvroBytesArray(builder.bytes, builder.lengthInBytes, off, len, elementReader);
    }

    @Nullable
    private static RawFieldReader createRawFieldReader(Schema elementSchema) {
        // handle nullable
        if (elementSchema.getType() == Schema.Type.UNION) {
            RawFieldReader nested = createRawFieldReader(elementSchema.getTypes().get(1));
            if (nested == null) {
                return null;
            }
            return (decoder, builder) -> {
                int index = decoder.readInt();
                builder.addInt(index);
                if (index == 1) {
                    nested.read(decoder, builder);
                }
            };
        }

        switch (elementSchema.getType()) {
            case BOOLEAN:
                return (decoder, builder) -> builder.addBoolean(decoder.readBoolean());
            case INT:
                return (decoder, builder) -> builder.addInt(decoder.readInt());
            case LONG:
                return (decoder, builder) -> builder.addLong(decoder.readLong());
            case FLOAT:
                return (decoder, builder) -> builder.addFloat(decoder.readFloat());
            case DOUBLE:
                return (decoder, builder) -> builder.addDouble(decoder.readDouble());
            case STRING:
            case BYTES:
                return (decoder, builder) -> builder.addBytes(decoder);
            default:
                // unknown or unsupported
                return null;
        }
    }

    public void writeRawElements(BinaryEncoder encoder, int offset, int length) throws IOException {
        if (length == 0) {
            return;
        }

        checkRange(offset, length);
        int rawOffset = off.get(offset);
        int last = offset + length - 1;
        int rawLength = off.get(last) + len.get(last) - rawOffset;
        encoder.writeFixed(bytes, rawOffset, rawLength);
    }

    private void checkRange(int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > size()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid range offset %s and length %s for array size %s.",
                            offset, length, size()));
        }
    }

    public byte[] bytes() {
        return bytes;
    }

    public int lengthInBytes() {
        return lengthInBytes;
    }

    @Override
    public int size() {
        return off.size();
    }

    @Override
    public boolean isNullAt(int pos) {
        return decodedArray().isNullAt(pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        return decodedArray().getBoolean(pos);
    }

    @Override
    public byte getByte(int pos) {
        return decodedArray().getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        return decodedArray().getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        return decodedArray().getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        return decodedArray().getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        return decodedArray().getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        return decodedArray().getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return decodedArray().getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return decodedArray().getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return decodedArray().getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        return decodedArray().getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return decodedArray().getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return decodedArray().getBlob(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return decodedArray().getArray(pos);
    }

    @Override
    public InternalVector getVector(int pos) {
        return decodedArray().getVector(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return decodedArray().getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return decodedArray().getRow(pos, numFields);
    }

    @Override
    public boolean[] toBooleanArray() {
        return decodedArray().toBooleanArray();
    }

    @Override
    public byte[] toByteArray() {
        return decodedArray().toByteArray();
    }

    @Override
    public short[] toShortArray() {
        return decodedArray().toShortArray();
    }

    @Override
    public int[] toIntArray() {
        return decodedArray().toIntArray();
    }

    @Override
    public long[] toLongArray() {
        return decodedArray().toLongArray();
    }

    @Override
    public float[] toFloatArray() {
        return decodedArray().toFloatArray();
    }

    @Override
    public double[] toDoubleArray() {
        return decodedArray().toDoubleArray();
    }

    private GenericArray decodedArray() {
        if (decodedArray != null) {
            return decodedArray;
        }

        try {
            BinaryDecoder decoder =
                    DecoderFactory.get().binaryDecoder(bytes, 0, lengthInBytes, null);
            Object[] values = new Object[size()];
            for (int i = 0; i < size(); i++) {
                values[i] = elementReader.read(decoder, null);
            }
            decodedArray = new GenericArray(values);
            return decodedArray;
        } catch (IOException e) {
            throw new RuntimeException("Failed to decode Avro array bytes.", e);
        }
    }

    /** Read the raw bytes from decoder and add to the {@link BytesBuilder}. */
    private interface RawFieldReader {

        void read(BinaryDecoder decoder, BytesBuilder builder) throws IOException;
    }

    /**
     * A growable byte buffer used to copy Avro binary-encoded element payloads from a decoder.
     *
     * <p>Please see the #encodeX method comments to know how many spaces should be ensured.
     */
    private static class BytesBuilder {

        private byte[] bytes = new byte[256];
        private int lengthInBytes;

        void addBoolean(boolean value) {
            ensure(1);
            lengthInBytes += BinaryData.encodeBoolean(value, bytes, lengthInBytes);
        }

        void addInt(int value) {
            ensure(5);
            lengthInBytes += BinaryData.encodeInt(value, bytes, lengthInBytes);
        }

        void addLong(long value) {
            ensure(10);
            lengthInBytes += BinaryData.encodeLong(value, bytes, lengthInBytes);
        }

        void addFloat(float value) {
            ensure(4);
            lengthInBytes += BinaryData.encodeFloat(value, bytes, lengthInBytes);
        }

        void addDouble(double value) {
            ensure(8);
            lengthInBytes += BinaryData.encodeDouble(value, bytes, lengthInBytes);
        }

        void addBytes(BinaryDecoder decoder) throws IOException {
            // BinaryDecoder#readString,readBytes
            int l = (int) decoder.readLong();
            ensure(l + 10);
            lengthInBytes += BinaryData.encodeLong(l, bytes, lengthInBytes);
            decoder.readFixed(bytes, lengthInBytes, l);
            lengthInBytes += l;
        }

        private void ensure(int need) {
            if (lengthInBytes + need <= bytes.length) {
                return;
            }

            int cap = bytes.length;
            while (lengthInBytes + need > cap) {
                cap *= 2;
            }

            byte[] newBytes = new byte[cap];
            System.arraycopy(bytes, 0, newBytes, 0, lengthInBytes);
            bytes = newBytes;
        }
    }
}
