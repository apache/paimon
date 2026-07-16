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

package org.apache.paimon.data;

import org.apache.paimon.data.variant.Variant;

import java.io.Serializable;

/**
 * Placeholder for an array blob field in data-evolution blob files. It should never be exposed to
 * users.
 */
public final class BlobArrayPlaceholder implements InternalArray, Serializable {

    private static final long serialVersionUID = 1L;

    public static final BlobArrayPlaceholder INSTANCE = new BlobArrayPlaceholder();

    private BlobArrayPlaceholder() {}

    private Object readResolve() {
        return INSTANCE;
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException(
                "Should never call this method for placeholder blob array.");
    }

    @Override
    public int size() {
        throw unsupported();
    }

    @Override
    public boolean isNullAt(int pos) {
        throw unsupported();
    }

    @Override
    public boolean getBoolean(int pos) {
        throw unsupported();
    }

    @Override
    public byte getByte(int pos) {
        throw unsupported();
    }

    @Override
    public short getShort(int pos) {
        throw unsupported();
    }

    @Override
    public int getInt(int pos) {
        throw unsupported();
    }

    @Override
    public long getLong(int pos) {
        throw unsupported();
    }

    @Override
    public float getFloat(int pos) {
        throw unsupported();
    }

    @Override
    public double getDouble(int pos) {
        throw unsupported();
    }

    @Override
    public BinaryString getString(int pos) {
        throw unsupported();
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw unsupported();
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw unsupported();
    }

    @Override
    public byte[] getBinary(int pos) {
        throw unsupported();
    }

    @Override
    public Variant getVariant(int pos) {
        throw unsupported();
    }

    @Override
    public Blob getBlob(int pos) {
        throw unsupported();
    }

    @Override
    public InternalArray getArray(int pos) {
        throw unsupported();
    }

    @Override
    public InternalVector getVector(int pos) {
        throw unsupported();
    }

    @Override
    public InternalMap getMap(int pos) {
        throw unsupported();
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw unsupported();
    }

    @Override
    public boolean[] toBooleanArray() {
        throw unsupported();
    }

    @Override
    public byte[] toByteArray() {
        throw unsupported();
    }

    @Override
    public short[] toShortArray() {
        throw unsupported();
    }

    @Override
    public int[] toIntArray() {
        throw unsupported();
    }

    @Override
    public long[] toLongArray() {
        throw unsupported();
    }

    @Override
    public float[] toFloatArray() {
        throw unsupported();
    }

    @Override
    public double[] toDoubleArray() {
        throw unsupported();
    }
}
