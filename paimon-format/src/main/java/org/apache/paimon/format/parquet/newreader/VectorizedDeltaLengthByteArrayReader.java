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

import org.apache.paimon.data.columnar.heap.HeapIntVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * An implementation of the Parquet DELTA_LENGTH_BYTE_ARRAY decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaLengthByteArrayReader extends VectorizedReaderBase
        implements VectorizedValuesReader {

    private final VectorizedDeltaBinaryPackedReader lengthReader;
    private ByteBufferInputStream in;
    private HeapIntVector lengthsVector;
    private int currentRow = 0;

    VectorizedDeltaLengthByteArrayReader() {
        lengthReader = new VectorizedDeltaBinaryPackedReader();
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
        lengthsVector = new HeapIntVector(valueCount);
        lengthReader.initFromPage(valueCount, in);
        lengthReader.readIntegers(lengthReader.getTotalValueCount(), lengthsVector, 0);
        this.in = in.remainingStream();
    }

    @Override
    public void readBinary(int total, WritableBytesVector c, int rowId) {
        ByteBuffer buffer;
        ByteBufferOutputWriter outputWriter = ByteBufferOutputWriter::writeArrayByteBuffer;
        int length;
        for (int i = 0; i < total; i++) {
            length = lengthsVector.getInt(rowId + i);
            try {
                buffer = in.slice(length);
            } catch (EOFException e) {
                throw new ParquetDecodingException("Failed to read " + length + " bytes");
            }
            outputWriter.write(c, rowId + i, buffer, length);
        }
        currentRow += total;
    }

    public ByteBuffer getBytes(int rowId) {
        int length = lengthsVector.getInt(rowId);
        try {
            return in.slice(length);
        } catch (EOFException e) {
            throw new ParquetDecodingException("Failed to read " + length + " bytes");
        }
    }

    @Override
    public void skipBinary(int total) {
        for (int i = 0; i < total; i++) {
            int remaining = lengthsVector.getInt(currentRow + i);
            while (remaining > 0) {
                remaining -= in.skip(remaining);
            }
        }
        currentRow += total;
    }
}
