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

import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.heap.HeapLongVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.io.ParquetDecodingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Test for delta encoding. */
abstract class DeltaEncodingTest<T extends Number, F extends WritableColumnVector> {

    protected int blockSize = 128;
    protected int miniBlockNum = 4;
    protected VectorizedDeltaBinaryPackedReader reader;
    protected F writableColumnVector;
    protected ValuesWriter writer;
    protected Random random;

    protected abstract void writeData(T[] data);

    protected abstract void writeData(T[] data, int length);

    protected abstract void initValuesWriter();

    protected abstract T[] allocDataArray(int size);

    protected abstract T getNextRandom();

    protected abstract T getTypeMinValue();

    protected abstract T getTypeMaxValue();

    protected abstract void readData(int total, F columnVector, int rowId);

    protected abstract void skip(int total);

    protected abstract T readDataFromVector(F columnVector, int rowId);

    protected abstract double estimatedSize(int length);

    protected abstract void setValue(T[] arr, int index, int value);

    protected abstract boolean compareValues(int expected, T actual);

    abstract F getWritableColumnVector(int size);

    @BeforeEach
    public void beforeEach() {
        random = new Random(0);
        initValuesWriter();
    }

    @Test
    public void testReadWhenDataIsAlignedWithBlock() {
        T[] data = allocDataArray(5 * blockSize);
        for (int i = 0; i < blockSize * 5; i++) {
            data[i] = getNextRandom();
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenBlockIsNotFullyWritten() {
        T[] data = allocDataArray(blockSize - 3);
        for (int i = 0; i < data.length; i++) {
            data[i] = getNextRandom();
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenMiniBlockIsNotFullyWritten() {
        int miniBlockSize = blockSize / miniBlockNum;
        T[] data = allocDataArray(miniBlockSize - 3);
        for (int i = 0; i < data.length; i++) {
            data[i] = getNextRandom();
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWithNegativeDeltas() {
        T[] data = allocDataArray(blockSize);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, 10 - (i * 32 - random.nextInt(6)));
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenDeltasAreSame() {
        T[] data = allocDataArray(2 * blockSize);
        for (int i = 0; i < blockSize; i++) {
            setValue(data, i, i * 32);
        }
        for (int i = blockSize; i < 2 * blockSize; i++) {
            setValue(data, i, 0);
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenValuesAreSame() {
        T[] data = allocDataArray(2 * blockSize);
        for (int i = 0; i < blockSize; i++) {
            setValue(data, i, 3);
        }
        for (int i = blockSize; i < 2 * blockSize; i++) {
            setValue(data, i, 0);
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenDeltaIs0ForEachBlock() {
        T[] data = allocDataArray(5 * blockSize + 1);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, (i - 1) / blockSize);
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadWhenDataIsNotAlignedWithBlock() {
        T[] data = allocDataArray(5 * blockSize + 3);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, random.nextInt(20) - 10);
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testReadMaxMinValue() {
        T[] data = allocDataArray(10);
        for (int i = 0; i < data.length; i++) {
            data[i] = (i % 2 == 0) ? getTypeMinValue() : getTypeMaxValue();
        }
        shouldWriteAndRead(data);
    }

    @Test
    public void testThrowExceptionWhenReadMoreThanWritten() {
        T[] data = allocDataArray(5 * blockSize + 1);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, i * 32);
        }
        shouldWriteAndRead(data);

        ParquetDecodingException e =
                assertThrows(
                        ParquetDecodingException.class,
                        () -> readData(1, writableColumnVector, data.length));
        assertTrue(e.getMessage().startsWith("No more values to read."));
    }

    @Test
    public void testSkip() throws IOException {
        T[] data = allocDataArray(5 * blockSize + 1);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, i * 32);
        }
        writeData(data);
        reader = new VectorizedDeltaBinaryPackedReader();
        reader.initFromPage(100, writer.getBytes().toInputStream());
        writableColumnVector = getWritableColumnVector(data.length);

        for (int i = 0; i < data.length; i++) {
            if (i % 3 == 0) {
                skip(1);
            } else {
                readData(1, writableColumnVector, i);
                assertTrue(compareValues(i * 32, readDataFromVector(writableColumnVector, i)));
            }
        }
    }

    @Test
    public void testSkipN() throws IOException {
        T[] data = allocDataArray(5 * blockSize + 1);
        for (int i = 0; i < data.length; i++) {
            setValue(data, i, i * 32);
        }
        writeData(data);
        reader = new VectorizedDeltaBinaryPackedReader();
        reader.initFromPage(100, writer.getBytes().toInputStream());
        writableColumnVector = getWritableColumnVector(data.length);
        int skipCount;
        for (int i = 0; i < data.length; ) {
            skipCount = (data.length - i) / 2;
            readData(1, writableColumnVector, i);
            assertTrue(compareValues(i * 32, readDataFromVector(writableColumnVector, i)));
            skip(skipCount);

            i += skipCount + 1;
        }
    }

    @Test
    public void testRandomDataTest() throws IOException {
        int maxSize = 1000;
        T[] data = allocDataArray(maxSize);
        for (int round = 0; round < 10; round++) {
            int size = random.nextInt(maxSize);
            for (int i = 0; i < size; i++) {
                data[i] = getNextRandom();
            }
            shouldReadAndWrite(data, size);
        }
    }

    private void shouldWriteAndRead(T[] data) {
        try {
            shouldReadAndWrite(data, data.length);
        } catch (IOException e) {
            fail("Failed to read or write data: " + e.getMessage());
        }
    }

    private void shouldReadAndWrite(T[] data, int length) throws IOException {
        for (boolean useDirect : new boolean[] {true, false}) {
            writeData(data, length);
            reader = new VectorizedDeltaBinaryPackedReader();
            byte[] page = writer.getBytes().toByteArray();

            assertTrue(estimatedSize(length) >= page.length);
            writableColumnVector = getWritableColumnVector(length);

            ByteBuffer buf =
                    useDirect
                            ? ByteBuffer.allocateDirect(page.length)
                            : ByteBuffer.allocate(page.length);
            buf.put(page);
            buf.flip();

            reader.initFromPage(100, ByteBufferInputStream.wrap(buf));

            readData(length, writableColumnVector, 0);

            for (int i = 0; i < length; i++) {
                if (!data[i].equals(readDataFromVector(writableColumnVector, i))) {
                    fail(
                            "Data mismatch at index "
                                    + i
                                    + ": Expected "
                                    + data[i]
                                    + ", but got "
                                    + readDataFromVector(writableColumnVector, i));
                }
            }
            writer.reset();
        }
    }

    // Implementations for Integer
    public static class ParquetDeltaEncodingIntegerTest
            extends DeltaEncodingTest<Integer, WritableIntVector> {

        @Override
        protected void writeData(Integer[] data) {
            writeData(data, data.length);
        }

        @Override
        protected void writeData(Integer[] data, int length) {
            for (int i = 0; i < length; i++) {
                writer.writeInteger(data[i]);
            }
        }

        @Override
        protected void initValuesWriter() {
            writer =
                    new DeltaBinaryPackingValuesWriterForInteger(
                            blockSize, miniBlockNum, 100, 200, new DirectByteBufferAllocator());
        }

        @Override
        protected Integer[] allocDataArray(int size) {
            return new Integer[size];
        }

        @Override
        protected Integer getNextRandom() {
            return random.nextInt();
        }

        @Override
        protected Integer getTypeMinValue() {
            return Integer.MIN_VALUE;
        }

        @Override
        protected Integer getTypeMaxValue() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected void readData(int total, WritableIntVector columnVector, int rowId) {
            reader.readIntegers(total, columnVector, rowId);
        }

        @Override
        protected void skip(int total) {
            reader.skipIntegers(total);
        }

        @Override
        protected Integer readDataFromVector(WritableIntVector columnVector, int rowId) {
            return columnVector.getInt(rowId);
        }

        @Override
        protected double estimatedSize(int length) {
            int miniBlockSize = blockSize / miniBlockNum;
            double miniBlockFlushed = Math.ceil((double) (length - 1) / miniBlockSize);
            double blockFlushed = Math.ceil((double) (length - 1) / blockSize);
            return 4 * 5 /* blockHeader */
                    + 4 * miniBlockFlushed * miniBlockSize /* data(aligned to miniBlock) */
                    + blockFlushed * miniBlockNum /* bitWidth of mini blocks */
                    + (5.0 * blockFlushed) /* min delta for each block */;
        }

        @Override
        protected void setValue(Integer[] arr, int index, int value) {
            arr[index] = value;
        }

        @Override
        protected boolean compareValues(int expected, Integer actual) {
            return expected == actual;
        }

        @Override
        WritableIntVector getWritableColumnVector(int size) {
            return new HeapIntVector(size);
        }
    }

    public static class ParquetDeltaEncodingLong
            extends DeltaEncodingTest<Long, WritableLongVector> {

        @Override
        protected void writeData(Long[] data) {
            writeData(data, data.length);
        }

        @Override
        protected void writeData(Long[] data, int length) {
            for (int i = 0; i < length; i++) {
                writer.writeLong(data[i]);
            }
        }

        @Override
        protected void initValuesWriter() {
            writer =
                    new DeltaBinaryPackingValuesWriterForLong(
                            blockSize, miniBlockNum, 100, 200, new DirectByteBufferAllocator());
        }

        @Override
        protected Long[] allocDataArray(int size) {
            return new Long[size];
        }

        @Override
        protected Long getNextRandom() {
            return random.nextLong();
        }

        @Override
        protected Long getTypeMinValue() {
            return Long.MIN_VALUE;
        }

        @Override
        protected Long getTypeMaxValue() {
            return Long.MAX_VALUE;
        }

        @Override
        protected void readData(int total, WritableLongVector columnVector, int rowId) {
            reader.readLongs(total, columnVector, rowId);
        }

        @Override
        protected void skip(int total) {
            reader.skipLongs(total);
        }

        @Override
        protected Long readDataFromVector(WritableLongVector columnVector, int rowId) {
            return columnVector.getLong(rowId);
        }

        @Override
        protected double estimatedSize(int length) {
            int miniBlockSize = blockSize / miniBlockNum;
            double miniBlockFlushed = Math.ceil((double) (length - 1) / miniBlockSize);
            double blockFlushed = Math.ceil((double) (length - 1) / blockSize);
            return 3 * 5
                    + 1 * 10 /* blockHeader + 3 * int + 1 * long */
                    + 8 * miniBlockFlushed * miniBlockSize /* data(aligned to miniBlock) */
                    + blockFlushed * miniBlockNum /* bitWidth of mini blocks */
                    + (10.0 * blockFlushed) /* min delta for each block */;
        }

        @Override
        protected void setValue(Long[] arr, int index, int value) {
            arr[index] = (long) value;
        }

        @Override
        protected boolean compareValues(int expected, Long actual) {
            return (long) expected == actual;
        }

        @Override
        WritableLongVector getWritableColumnVector(int size) {
            return new HeapLongVector(size);
        }
    }
}
