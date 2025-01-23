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

import org.apache.paimon.data.columnar.writable.WritableBooleanVector;
import org.apache.paimon.data.columnar.writable.WritableByteVector;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableDoubleVector;
import org.apache.paimon.data.columnar.writable.WritableFloatVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.data.columnar.writable.WritableShortVector;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Base class for implementations of VectorizedValuesReader. Mainly to avoid duplication of methods
 * that are not supported by concrete implementations
 */
public class VectorizedReaderBase extends ValuesReader implements VectorizedValuesReader {

    @Override
    public void skip() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
        throw new UnsupportedOperationException();
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Binary readBinary(int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBooleans(int total, WritableBooleanVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBytes(int total, WritableByteVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readShorts(int total, WritableShortVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readIntegers(int total, WritableIntVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readLongs(int total, WritableLongVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFloats(int total, WritableFloatVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readDoubles(int total, WritableDoubleVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBinary(int total, WritableBytesVector c, int rowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipBooleans(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipBytes(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipShorts(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipIntegers(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipLongs(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipFloats(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipDoubles(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipBinary(int total) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void skipFixedLenByteArray(int total, int len) {
        throw new UnsupportedOperationException();
    }
}
