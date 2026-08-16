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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryArrayWriter;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.IntArrayList;

import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryDecoder;

import java.io.IOException;

/** An {@link InternalMap} stored in avro format bytes. */
public class AvroBytesStringMap implements InternalMap {

    private static final int ELEMENT_SIZE =
            BinaryArray.calculateFixLengthPartSize(DataTypes.STRING());

    // stores original bytes from avro files
    private final byte[] bytes;
    // total bytes length
    private final int lengthInBytes;
    // offset of each string
    private final IntArrayList off;
    // length of each string
    private final IntArrayList len;

    private BinaryArray keyArray;
    private BinaryArray valueArray;

    private AvroBytesStringMap(
            byte[] bytes, int lengthInBytes, IntArrayList off, IntArrayList len) {
        this.bytes = bytes;
        this.lengthInBytes = lengthInBytes;
        this.off = off;
        this.len = len;
    }

    public static AvroBytesStringMap create(BinaryDecoder decoder, boolean valueNullable)
            throws IOException {
        byte[] bytes = new byte[256];
        int lengthInBytes = 0;
        IntArrayList off = new IntArrayList(16);
        IntArrayList len = new IntArrayList(16);

        long chunkLength = decoder.readMapStart();
        while (chunkLength > 0) {
            for (int i = 0; i < chunkLength; i++) {
                // https://github.com/apache/avro/blob/6db1f79e22e8558ac0455cf73f6e1fb7d1139f44/lang/java/avro/src/main/java/org/apache/avro/io/BinaryDecoder.java#L296
                int l = (int) decoder.readLong();
                bytes = ensure(bytes, lengthInBytes, l + 10);
                lengthInBytes += BinaryData.encodeLong(l, bytes, lengthInBytes);
                decoder.readFixed(bytes, lengthInBytes, l);
                off.add(lengthInBytes);
                len.add(l);
                lengthInBytes += l;

                // https://github.com/apache/avro/blob/6db1f79e22e8558ac0455cf73f6e1fb7d1139f44/lang/java/avro/src/main/java/org/apache/avro/io/BinaryDecoder.java#L479
                int flag = 1;
                if (valueNullable) {
                    flag = decoder.readInt();
                    if (flag == 0) {
                        off.add(-1);
                        len.add(-1);
                    }
                    bytes = ensure(bytes, lengthInBytes, 5);
                    lengthInBytes += BinaryData.encodeInt(flag, bytes, lengthInBytes);
                }

                if (flag != 0) {
                    l = (int) decoder.readLong();
                    bytes = ensure(bytes, lengthInBytes, l + 10);
                    lengthInBytes += BinaryData.encodeLong(l, bytes, lengthInBytes);
                    decoder.readFixed(bytes, lengthInBytes, l);
                    off.add(lengthInBytes);
                    len.add(l);
                    lengthInBytes += l;
                }
            }
            chunkLength = decoder.mapNext();
        }

        return new AvroBytesStringMap(bytes, lengthInBytes, off, len);
    }

    private static byte[] ensure(byte[] bytes, int lengthInBytes, int need) {
        if (lengthInBytes + need <= bytes.length) {
            return bytes;
        }

        int cap = bytes.length;
        while (lengthInBytes + need > cap) {
            cap *= 2;
        }

        byte[] newBytes = new byte[cap];
        System.arraycopy(bytes, 0, newBytes, 0, lengthInBytes);
        return newBytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    public int lengthInBytes() {
        return lengthInBytes;
    }

    @Override
    public int size() {
        return off.size() / 2;
    }

    @Override
    public InternalArray keyArray() {
        decode();
        return keyArray;
    }

    @Override
    public InternalArray valueArray() {
        decode();
        return valueArray;
    }

    private void decode() {
        if (keyArray != null) {
            return;
        }

        int size = size();
        keyArray = new BinaryArray();
        BinaryArrayWriter keyWriter = new BinaryArrayWriter(keyArray, size, ELEMENT_SIZE);
        valueArray = new BinaryArray();
        BinaryArrayWriter valueWriter = new BinaryArrayWriter(valueArray, size, ELEMENT_SIZE);

        for (int i = 0, j = 0; i < size; i++) {
            keyWriter.writeBinary(i, bytes, off.get(j), len.get(j));
            j++;
            int o = off.get(j);
            if (o == -1) {
                valueWriter.setNullAt(i);
            } else {
                valueWriter.writeBinary(i, bytes, o, len.get(j));
            }
            j++;
        }

        keyWriter.complete();
        valueWriter.complete();
    }
}
