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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

/**
 * An implementation of {@link InternalVector} which provides a casted view of the underlying {@link
 * InternalVector}.
 *
 * <p>It reads data from underlying {@link InternalVector} according to source logical type and
 * casts it with specific {@link CastExecutor}.
 */
public class CastedVector extends CastedArray implements InternalVector {

    protected CastedVector(CastElementGetter castElementGetter) {
        super(castElementGetter);
    }

    /**
     * Replaces the underlying {@link InternalVector} backing this {@link CastedVector}.
     *
     * <p>This method replaces the vector in place and does not return a new object. This is done
     * for performance reasons.
     */
    public static CastedVector from(CastElementGetter castElementGetter) {
        return new CastedVector(castElementGetter);
    }

    public CastedVector replaceVector(InternalVector vector) {
        super.replaceArray(vector);
        return this;
    }

    @Override
    public CastedArray replaceArray(InternalArray array) {
        throw new IllegalArgumentException("CastedVector does not support replaceArray.");
    }

    @Override
    public BinaryString getString(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support String.");
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        throw new UnsupportedOperationException("CastedVector does not support Decimal.");
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        throw new UnsupportedOperationException("CastedVector does not support Timestamp.");
    }

    @Override
    public byte[] getBinary(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support Binary.");
    }

    @Override
    public Variant getVariant(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support Variant.");
    }

    @Override
    public Blob getBlob(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support Blob.");
    }

    @Override
    public InternalArray getArray(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support nested Array.");
    }

    @Override
    public InternalVector getVector(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support nested VectorType.");
    }

    @Override
    public InternalMap getMap(int pos) {
        throw new UnsupportedOperationException("CastedVector does not support Map.");
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        throw new UnsupportedOperationException("CastedVector does not support nested Row.");
    }
}
