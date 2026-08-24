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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.columnar.writable.WritableByteVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableDoubleVector;
import org.apache.paimon.data.columnar.writable.WritableFloatVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Vectorized reader for Parquet byte stream split encoding. */
public class VectorizedByteStreamSplitValuesReader extends VectorizedReaderBase {

    private final int typeWidth;

    private int valueCount;

    private byte[] pageData;

    private int offset;

    public VectorizedByteStreamSplitValuesReader(int typeWidth) {
        this.typeWidth = typeWidth;
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
        int totalBytes = in.available();
        this.valueCount = totalBytes / typeWidth;
        this.offset = 0;
        this.pageData = new byte[totalBytes];
        ByteBuffer buf = in.slice(totalBytes);
        buf.get(pageData, 0, totalBytes);
    }

    private int assembleInt(int idx) {
        return (pageData[idx] & 0xFF)
                | ((pageData[valueCount + idx] & 0xFF) << 8)
                | ((pageData[2 * valueCount + idx] & 0xFF) << 16)
                | ((pageData[3 * valueCount + idx] & 0xFF) << 24);
    }

    private long assembleLong(int idx) {
        return (pageData[idx] & 0xFFL)
                | ((pageData[valueCount + idx] & 0xFFL) << 8)
                | ((pageData[2 * valueCount + idx] & 0xFFL) << 16)
                | ((pageData[3 * valueCount + idx] & 0xFFL) << 24)
                | ((pageData[4 * valueCount + idx] & 0xFFL) << 32)
                | ((pageData[5 * valueCount + idx] & 0xFFL) << 40)
                | ((pageData[6 * valueCount + idx] & 0xFFL) << 48)
                | ((pageData[7 * valueCount + idx] & 0xFFL) << 56);
    }

    @Override
    public byte readByte() {
        return (byte) readInteger();
    }

    @Override
    public short readShort() {
        return (short) readInteger();
    }

    @Override
    public int readInteger() {
        return assembleInt(offset++);
    }

    @Override
    public long readLong() {
        return assembleLong(offset++);
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(assembleInt(offset++));
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(assembleLong(offset++));
    }

    @Override
    public Binary readBinary(int len) {
        byte[] result = new byte[len];
        for (int b = 0; b < len; b++) {
            result[b] = pageData[b * valueCount + offset];
        }
        offset++;
        return Binary.fromConstantByteArray(result);
    }

    @Override
    public void readBytes(int total, WritableByteVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setByte(rowId + i, (byte) assembleInt(offset + i));
        }
        offset += total;
    }

    @Override
    public void readShorts(int total, WritableShortVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setShort(rowId + i, (short) assembleInt(offset + i));
        }
        offset += total;
    }

    @Override
    public void readIntegers(int total, WritableIntVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setInt(rowId + i, assembleInt(offset + i));
        }
        offset += total;
    }

    @Override
    public void readIntegersAsLongs(int total, WritableLongVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setLong(rowId + i, assembleInt(offset + i));
        }
        offset += total;
    }

    @Override
    public void readIntegersAsDoubles(int total, WritableDoubleVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setDouble(rowId + i, (double) assembleInt(offset + i));
        }
        offset += total;
    }

    @Override
    public void readLongs(int total, WritableLongVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setLong(rowId + i, assembleLong(offset + i));
        }
        offset += total;
    }

    @Override
    public void readLongsAsInts(int total, WritableIntVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setInt(rowId + i, (int) assembleLong(offset + i));
        }
        offset += total;
    }

    @Override
    public void readFloats(int total, WritableFloatVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setFloat(rowId + i, Float.intBitsToFloat(assembleInt(offset + i)));
        }
        offset += total;
    }

    @Override
    public void readFloatsAsDoubles(int total, WritableDoubleVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setDouble(rowId + i, (double) Float.intBitsToFloat(assembleInt(offset + i)));
        }
        offset += total;
    }

    @Override
    public void readDoubles(int total, WritableDoubleVector c, int rowId) {
        for (int i = 0; i < total; i++) {
            c.setDouble(rowId + i, Double.longBitsToDouble(assembleLong(offset + i)));
        }
        offset += total;
    }

    @Override
    public void readBinary(int total, WritableBytesVector c, int rowId) {
        byte[] scratch = new byte[typeWidth];
        for (int i = 0; i < total; i++) {
            for (int b = 0; b < typeWidth; b++) {
                scratch[b] = pageData[b * valueCount + offset];
            }
            c.putByteArray(rowId + i, scratch, 0, typeWidth);
            offset++;
        }
    }

    @Override
    public void readFixedLenByteArray(int total, int len, WritableBytesVector c, int rowId) {
        readBinary(total, c, rowId);
    }

    @Override
    public void skipBytes(int total) {
        offset += total;
    }

    @Override
    public void skipShorts(int total) {
        offset += total;
    }

    @Override
    public void skipIntegers(int total) {
        offset += total;
    }

    @Override
    public void skipLongs(int total) {
        offset += total;
    }

    @Override
    public void skipFloats(int total) {
        offset += total;
    }

    @Override
    public void skipDoubles(int total) {
        offset += total;
    }

    @Override
    public void skipBinary(int total) {
        offset += total;
    }

    @Override
    public void skipFixedLenByteArray(int total, int len) {
        offset += total;
    }
}
