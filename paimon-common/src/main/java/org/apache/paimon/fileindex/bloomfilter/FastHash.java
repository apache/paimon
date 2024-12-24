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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import net.openhft.hashing.LongHashFunction;

/** Hash object to 64 bits hash code. */
public interface FastHash {

    long hash(Object o);

    static FastHash getHashFunction(DataType type) {
        return type.accept(FastHashVisitor.INSTANCE);
    }

    /** Fast hash for object differs to DataType. */
    class FastHashVisitor implements DataTypeVisitor<FastHash> {

        private static final FastHashVisitor INSTANCE = new FastHashVisitor();

        @Override
        public FastHash visit(CharType charType) {
            return o -> hash64(((BinaryString) o).toBytes());
        }

        @Override
        public FastHash visit(VarCharType varCharType) {
            return o -> hash64(((BinaryString) o).toBytes());
        }

        @Override
        public FastHash visit(BooleanType booleanType) {
            throw new UnsupportedOperationException("Does not support type boolean");
        }

        @Override
        public FastHash visit(BinaryType binaryType) {
            return o -> hash64((byte[]) o);
        }

        @Override
        public FastHash visit(VarBinaryType varBinaryType) {
            return o -> hash64((byte[]) o);
        }

        @Override
        public FastHash visit(DecimalType decimalType) {
            throw new UnsupportedOperationException("Does not support decimal");
        }

        @Override
        public FastHash visit(TinyIntType tinyIntType) {
            return o -> getLongHash((byte) o);
        }

        @Override
        public FastHash visit(SmallIntType smallIntType) {
            return o -> getLongHash((short) o);
        }

        @Override
        public FastHash visit(IntType intType) {
            return o -> getLongHash((int) o);
        }

        @Override
        public FastHash visit(BigIntType bigIntType) {
            return o -> getLongHash((long) o);
        }

        @Override
        public FastHash visit(FloatType floatType) {
            return o -> getLongHash(Float.floatToIntBits((float) o));
        }

        @Override
        public FastHash visit(DoubleType doubleType) {
            return o -> getLongHash(Double.doubleToLongBits((double) o));
        }

        @Override
        public FastHash visit(DateType dateType) {
            return o -> getLongHash((int) o);
        }

        @Override
        public FastHash visit(TimeType timeType) {
            return o -> getLongHash((int) o);
        }

        @Override
        public FastHash visit(TimestampType timestampType) {
            final int precision = timestampType.getPrecision();
            return o -> {
                if (o == null) {
                    return 0;
                }
                if (precision <= 3) {
                    return getLongHash(((Timestamp) o).getMillisecond());
                }

                return getLongHash(((Timestamp) o).toMicros());
            };
        }

        @Override
        public FastHash visit(LocalZonedTimestampType localZonedTimestampType) {
            final int precision = localZonedTimestampType.getPrecision();
            return o -> {
                if (o == null) {
                    return 0;
                }
                if (precision <= 3) {
                    return getLongHash(((Timestamp) o).getMillisecond());
                }

                return getLongHash(((Timestamp) o).toMicros());
            };
        }

        @Override
        public FastHash visit(VariantType variantType) {
            throw new UnsupportedOperationException("Does not support type variant");
        }

        @Override
        public FastHash visit(ArrayType arrayType) {
            throw new UnsupportedOperationException("Does not support type array");
        }

        @Override
        public FastHash visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Does not support type mutiset");
        }

        @Override
        public FastHash visit(MapType mapType) {
            throw new UnsupportedOperationException("Does not support type map");
        }

        @Override
        public FastHash visit(RowType rowType) {
            throw new UnsupportedOperationException("Does not support type row");
        }

        // Thomas Wang's integer hash function
        // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
        static long getLongHash(long key) {
            key = (~key) + (key << 21); // key = (key << 21) - key - 1;
            key = key ^ (key >> 24);
            key = (key + (key << 3)) + (key << 8); // key * 265
            key = key ^ (key >> 14);
            key = (key + (key << 2)) + (key << 4); // key * 21
            key = key ^ (key >> 28);
            key = key + (key << 31);
            return key;
        }

        static long hash64(byte[] data) {
            return LongHashFunction.xx().hashBytes(data);
        }
    }
}
