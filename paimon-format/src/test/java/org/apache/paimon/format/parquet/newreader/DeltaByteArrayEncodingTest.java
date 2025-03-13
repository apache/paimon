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

package org.apache.paimon.format.parquet.newreader;

import org.apache.paimon.data.columnar.heap.HeapBytesVector;
import org.apache.paimon.data.columnar.heap.HeapIntVector;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Test for delta byte array encoding. */
public class DeltaByteArrayEncodingTest {

    private final String[] values = new String[] {"parquet-mr", "parquet", "parquet-format"};
    private String[] randValues;

    private DeltaByteArrayWriter writer;
    private VectorizedDeltaByteArrayReader reader;

    @BeforeEach
    public void beforeEach() {
        writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
        reader = new VectorizedDeltaByteArrayReader();
        randValues = Utils.getRandomStringSamples(10000, 32);
    }

    @Test
    public void testSerialization() throws Exception {
        assertReadWrite(writer, reader, values);
    }

    @Test
    public void randomStrings() throws Exception {
        assertReadWrite(writer, reader, randValues);
    }

    @Test
    public void randomStringsWithSkip() throws Exception {
        assertReadWriteWithSkip(writer, reader, randValues);
    }

    @Test
    public void randomStringsWithSkipN() throws Exception {
        assertReadWriteWithSkipN(writer, reader, randValues);
    }

    @Test
    public void testLengths() throws Exception {
        VectorizedDeltaBinaryPackedReader lenReader = new VectorizedDeltaBinaryPackedReader();
        Utils.writeData(writer, values);
        ByteBufferInputStream data = writer.getBytes().toInputStream();
        HeapIntVector writableColumnVector = new HeapIntVector(values.length);
        lenReader.initFromPage(values.length, data);
        lenReader.readIntegers(values.length, writableColumnVector, 0);
        assertEquals(0, writableColumnVector.getInt(0));
        assertEquals(7, writableColumnVector.getInt(1));
        assertEquals(7, writableColumnVector.getInt(2));

        lenReader = new VectorizedDeltaBinaryPackedReader();
        writableColumnVector = new HeapIntVector(values.length);
        lenReader.initFromPage(values.length, data);
        lenReader.readIntegers(values.length, writableColumnVector, 0);

        assertEquals(10, writableColumnVector.getInt(0));
        assertEquals(0, writableColumnVector.getInt(1));
        assertEquals(7, writableColumnVector.getInt(2));
    }

    @Test
    public void testNegativeSize() {
        HeapBytesVector heapBytesVector = new HeapBytesVector(32);
        assertThrows(
                RuntimeException.class,
                () -> heapBytesVector.putByteArray(1, null, 0, Integer.MAX_VALUE - 1),
                String.format(
                        "The new claimed capacity %s is too large, will overflow the INTEGER.MAX after multiply by 2. "
                                + "Try reduce `read.batch-size` to avoid this exception.",
                        Integer.MAX_VALUE - 1));
    }

    private void assertReadWrite(
            DeltaByteArrayWriter writer, VectorizedDeltaByteArrayReader reader, String[] vals)
            throws Exception {
        Utils.writeData(writer, vals);
        int length = vals.length;
        HeapBytesVector writableColumnVector = new HeapBytesVector(length);
        reader.initFromPage(length, writer.getBytes().toInputStream());
        reader.readBinary(length, writableColumnVector, 0);
        for (int i = 0; i < length; i++) {
            assertArrayEquals(vals[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
        }
    }

    private void assertReadWriteWithSkip(
            DeltaByteArrayWriter writer, VectorizedDeltaByteArrayReader reader, String[] vals)
            throws Exception {
        Utils.writeData(writer, vals);
        int length = vals.length;
        HeapBytesVector writableColumnVector = new HeapBytesVector(length);
        reader.initFromPage(length, writer.getBytes().toInputStream());

        for (int i = 0; i < vals.length; i += 2) {
            reader.readBinary(1, writableColumnVector, i);
            assertArrayEquals(vals[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
            reader.skipBinary(1);
        }
    }

    private void assertReadWriteWithSkipN(
            DeltaByteArrayWriter writer, VectorizedDeltaByteArrayReader reader, String[] vals)
            throws Exception {
        Utils.writeData(writer, vals);
        int length = vals.length;
        HeapBytesVector writableColumnVector = new HeapBytesVector(length);
        reader.initFromPage(length, writer.getBytes().toInputStream());

        for (int i = 0; i < vals.length; ) {
            int skipCount = (vals.length - i) / 2;
            reader.readBinary(1, writableColumnVector, i);
            assertArrayEquals(vals[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
            reader.skipBinary(skipCount);
            i += skipCount + 1;
        }
    }
}
