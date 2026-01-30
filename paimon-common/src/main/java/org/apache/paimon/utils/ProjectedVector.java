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

package org.apache.paimon.utils;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataType;

/**
 * An implementation of {@link InternalVector} which provides a projected view of the underlying
 * {@link InternalVector}.
 *
 * <p>Projection includes both reducing the accessible fields and reordering them.
 *
 * <p>Note: This class supports only top-level projections, not nested projections.
 */
public class ProjectedVector extends ProjectedArray implements InternalVector {

    protected ProjectedVector(int[] indexMapping) {
        super(indexMapping);
    }

    /**
     * Replaces the underlying {@link InternalVector} backing this {@link ProjectedVector}.
     *
     * <p>This method replaces the row data in place and does not return a new object. This is done
     * for performance reasons.
     */
    public ProjectedVector replaceVector(InternalVector vector) {
        super.replaceArray(vector);
        return this;
    }

    @Override
    public ProjectedArray replaceArray(InternalArray array) {
        throw new IllegalArgumentException("ProjectedVector does not support replaceArray.");
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public BinaryString getString(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support String.");
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException("ProjectedVector does not support Decimal.");
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("ProjectedVector does not support Timestamp.");
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support Binary.");
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support Variant.");
    }

    @Override
    public Blob getBlob(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support Blob.");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support nested Array.");
    }

    @Override
    public InternalVector getVector(int pos) {
        throw new UnsupportedOperationException(
                "ProjectedVector does not support nested VectorType.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("ProjectedVector does not support Map.");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("ProjectedVector does not support nested Row.");
    }

    /**
     * Create an empty {@link ProjectedVector} starting from a {@code projection} vector.
     *
     * <p>The vector represents the mapping of the fields of the original {@link DataType}. For
     * example, {@code [0, 2, 1]} specifies to include in the following order the 1st field, the 3rd
     * field and the 2nd field of the row.
     *
     * @see Projection
     * @see ProjectedVector
     */
    public static ProjectedVector from(int[] projection) {
        return new ProjectedVector(projection);
    }
}
