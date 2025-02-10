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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** Test for delta length byte array encoding. */
public class DeltaLengthByteArrayEncodingTest {

    private final String[] values = new String[] {"parquet", "hadoop", "mapreduce"};
    private DeltaLengthByteArrayValuesWriter writer;
    private VectorizedDeltaLengthByteArrayReader reader;

    @BeforeEach
    public void beforeEach() {
        writer =
                new DeltaLengthByteArrayValuesWriter(
                        64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
        reader = new VectorizedDeltaLengthByteArrayReader();
    }

    @Test
    public void testSerialization() throws Exception {
        writeData(writer, values);
        readAndValidate(reader, writer.getBytes().toInputStream(), values.length, values);
    }

    @Test
    public void testRandomStrings() throws Exception {
        String[] values = Utils.getRandomStringSamples(1000, 32);
        writeData(writer, values);
        readAndValidate(reader, writer.getBytes().toInputStream(), values.length, values);
    }

    @Test
    public void testRandomStringsWithEmptyStrings() throws Exception {
        String[] values = getRandomStringSamplesWithEmptyStrings(1000, 32);
        writeData(writer, values);
        readAndValidate(reader, writer.getBytes().toInputStream(), values.length, values);
    }

    @Test
    public void testSkipWithRandomStrings() throws Exception {
        String[] values = Utils.getRandomStringSamples(1000, 32);
        writeData(writer, values);
        reader.initFromPage(values.length, writer.getBytes().toInputStream());
        HeapBytesVector writableColumnVector = new HeapBytesVector(values.length);

        for (int i = 0; i < values.length; i += 2) {
            reader.readBinary(1, writableColumnVector, i);
            assertArrayEquals(values[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
            reader.skipBinary(1);
        }

        reader = new VectorizedDeltaLengthByteArrayReader();
        reader.initFromPage(values.length, writer.getBytes().toInputStream());
        writableColumnVector = new HeapBytesVector(values.length);

        for (int i = 0; i < values.length; ) {
            int skipCount = (values.length - i) / 2;
            reader.readBinary(1, writableColumnVector, i);
            assertArrayEquals(values[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
            reader.skipBinary(skipCount);
            i += skipCount + 1;
        }
    }

    @Test
    public void testLengths() throws Exception {
        VectorizedDeltaBinaryPackedReader lenReader = new VectorizedDeltaBinaryPackedReader();
        writeData(writer, values);

        HeapIntVector writableColumnVector = new HeapIntVector(values.length);
        lenReader.initFromPage(values.length, writer.getBytes().toInputStream());
        lenReader.readIntegers(values.length, writableColumnVector, 0);

        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i].length(), writableColumnVector.getInt(i));
        }
    }

    private void writeData(DeltaLengthByteArrayValuesWriter writer, String[] values) {
        for (String value : values) {
            writer.writeBytes(Binary.fromString(value));
        }
    }

    private void readAndValidate(
            VectorizedDeltaLengthByteArrayReader reader,
            ByteBufferInputStream is,
            int length,
            String[] expectedValues)
            throws IOException {
        HeapBytesVector writableColumnVector = new HeapBytesVector(length);
        reader.initFromPage(length, is);
        reader.readBinary(length, writableColumnVector, 0);
        for (int i = 0; i < length; i++) {
            assertArrayEquals(
                    expectedValues[i].getBytes(), writableColumnVector.getBytes(i).getBytes());
        }
    }

    private String[] getRandomStringSamplesWithEmptyStrings(int numSamples, int maxLength) {
        Random randomLen = new Random();
        Random randomEmpty = new Random();
        String[] samples = new String[numSamples];

        for (int i = 0; i < numSamples; i++) {
            int maxLen = randomLen.nextInt(maxLength);
            if (randomEmpty.nextInt() % 11 != 0) {
                maxLen = 0;
            }
            samples[i] = RandomStringUtils.randomAlphanumeric(0, maxLen);
        }
        return samples;
    }
}
