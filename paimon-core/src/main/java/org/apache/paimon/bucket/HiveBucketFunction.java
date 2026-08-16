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

package org.apache.paimon.bucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

/** Hive-compatible bucket function. */
public class HiveBucketFunction implements BucketFunction {

    private static final long serialVersionUID = 1L;

    private static final int SEED = 0;

    private final InternalRow.FieldGetter[] fieldGetters;

    public HiveBucketFunction(RowType rowType) {
        this.fieldGetters = InternalRowUtils.createFieldGetters(rowType.getFieldTypes());
    }

    @Override
    public int bucket(BinaryRow row, int numBuckets) {
        assert numBuckets > 0 && row.getRowKind() == RowKind.INSERT : "Num bucket is illegal";

        int hash = SEED;
        for (int i = 0; i < row.getFieldCount(); i++) {
            hash = (31 * hash) + computeHash(fieldGetters[i].getFieldOrNull(row));
        }
        return mod(hash & Integer.MAX_VALUE, numBuckets);
    }

    static int mod(int value, int divisor) {
        int remainder = value % divisor;
        if (remainder < 0) {
            return (remainder + divisor) % divisor;
        }
        return remainder;
    }

    private int computeHash(Object value) {
        if (value == null) {
            return 0;
        }

        if (value instanceof Boolean) {
            return HiveHasher.hashInt((Boolean) value ? 1 : 0);
        } else if (value instanceof Byte) {
            return HiveHasher.hashInt(((Byte) value).intValue());
        } else if (value instanceof Short) {
            return HiveHasher.hashInt(((Short) value).intValue());
        } else if (value instanceof Integer) {
            return HiveHasher.hashInt((Integer) value);
        } else if (value instanceof Long) {
            return HiveHasher.hashLong((Long) value);
        } else if (value instanceof Float) {
            float floatValue = (Float) value;
            return HiveHasher.hashInt(floatValue == -0.0f ? 0 : Float.floatToIntBits(floatValue));
        } else if (value instanceof Double) {
            double doubleValue = (Double) value;
            return HiveHasher.hashLong(
                    doubleValue == -0.0d ? 0L : Double.doubleToLongBits(doubleValue));
        } else if (value instanceof BinaryString) {
            BinaryString stringValue = (BinaryString) value;
            return HiveHasher.hashUnsafeBytes(
                    stringValue.getSegments(),
                    stringValue.getOffset(),
                    stringValue.getSizeInBytes());
        } else if (value instanceof byte[]) {
            return HiveHasher.hashBytes((byte[]) value);
        } else if (value instanceof Decimal) {
            return HiveHasher.normalizeDecimal(((Decimal) value).toBigDecimal()).hashCode();
        }

        throw new UnsupportedOperationException(
                "Unsupported type as bucket key type " + value.getClass());
    }
}
