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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.utils.PartialRow;
import org.apache.paimon.utils.UriReaderFactory;

/** Reattaches a descriptor reader to BLOBs which passed through Flink row serialization. */
final class BlobDescriptorResolvingRow extends PartialRow {

    private final InternalRow wrapped;
    private final UriReaderFactory uriReaderFactory;

    BlobDescriptorResolvingRow(InternalRow wrapped, UriReaderFactory uriReaderFactory) {
        super(wrapped.getFieldCount(), wrapped);
        this.wrapped = wrapped;
        this.uriReaderFactory = uriReaderFactory;
    }

    @Override
    public BlobDescriptorResolvingRow replace(InternalRow row) {
        throw new UnsupportedOperationException("Replacing the wrapped row is not supported.");
    }

    @Override
    public int getFieldCount() {
        return wrapped.getFieldCount();
    }

    @Override
    public Blob getBlob(int pos) {
        return withReader(super.getBlob(pos), uriReaderFactory);
    }

    private static Blob withReader(Blob blob, UriReaderFactory uriReaderFactory) {
        if (!(blob instanceof BlobRef)) {
            return blob;
        }

        BlobDescriptor descriptor = blob.toDescriptor();
        return Blob.fromDescriptor(uriReaderFactory.create(descriptor.uri()), descriptor);
    }

    @Override
    public InternalArray getArray(int pos) {
        return new BlobDescriptorResolvingArray(super.getArray(pos), uriReaderFactory);
    }

    private static final class BlobDescriptorResolvingArray implements InternalArray {

        private final InternalArray wrapped;
        private final UriReaderFactory uriReaderFactory;

        private BlobDescriptorResolvingArray(
                InternalArray wrapped, UriReaderFactory uriReaderFactory) {
            this.wrapped = wrapped;
            this.uriReaderFactory = uriReaderFactory;
        }

        @Override
        public int size() {
            return wrapped.size();
        }

        @Override
        public boolean isNullAt(int pos) {
            return wrapped.isNullAt(pos);
        }

        @Override
        public boolean getBoolean(int pos) {
            return wrapped.getBoolean(pos);
        }

        @Override
        public byte getByte(int pos) {
            return wrapped.getByte(pos);
        }

        @Override
        public short getShort(int pos) {
            return wrapped.getShort(pos);
        }

        @Override
        public int getInt(int pos) {
            return wrapped.getInt(pos);
        }

        @Override
        public long getLong(int pos) {
            return wrapped.getLong(pos);
        }

        @Override
        public float getFloat(int pos) {
            return wrapped.getFloat(pos);
        }

        @Override
        public double getDouble(int pos) {
            return wrapped.getDouble(pos);
        }

        @Override
        public BinaryString getString(int pos) {
            return wrapped.getString(pos);
        }

        @Override
        public Decimal getDecimal(int pos, int precision, int scale) {
            return wrapped.getDecimal(pos, precision, scale);
        }

        @Override
        public Timestamp getTimestamp(int pos, int precision) {
            return wrapped.getTimestamp(pos, precision);
        }

        @Override
        public byte[] getBinary(int pos) {
            return wrapped.getBinary(pos);
        }

        @Override
        public Variant getVariant(int pos) {
            return wrapped.getVariant(pos);
        }

        @Override
        public Blob getBlob(int pos) {
            return withReader(wrapped.getBlob(pos), uriReaderFactory);
        }

        @Override
        public InternalArray getArray(int pos) {
            return new BlobDescriptorResolvingArray(wrapped.getArray(pos), uriReaderFactory);
        }

        @Override
        public InternalVector getVector(int pos) {
            return wrapped.getVector(pos);
        }

        @Override
        public InternalMap getMap(int pos) {
            return wrapped.getMap(pos);
        }

        @Override
        public InternalRow getRow(int pos, int numFields) {
            return wrapped.getRow(pos, numFields);
        }

        @Override
        public boolean[] toBooleanArray() {
            return wrapped.toBooleanArray();
        }

        @Override
        public byte[] toByteArray() {
            return wrapped.toByteArray();
        }

        @Override
        public short[] toShortArray() {
            return wrapped.toShortArray();
        }

        @Override
        public int[] toIntArray() {
            return wrapped.toIntArray();
        }

        @Override
        public long[] toLongArray() {
            return wrapped.toLongArray();
        }

        @Override
        public float[] toFloatArray() {
            return wrapped.toFloatArray();
        }

        @Override
        public double[] toDoubleArray() {
            return wrapped.toDoubleArray();
        }
    }
}
