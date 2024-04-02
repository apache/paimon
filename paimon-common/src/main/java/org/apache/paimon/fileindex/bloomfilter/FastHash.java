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

import java.util.function.Function;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.ObjectToBytesVisitor;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
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
import org.apache.paimon.utils.MurmurHashUtils;

/** Fast hash for number type. */
public class FastHash implements DataTypeVisitor<Function<Object, Integer>> {

    public static final FastHash INSTANCE = new FastHash();

    @Override
    public Function<Object, Integer> visit(CharType charType) {
        final Function<Object, byte[]> converter = charType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(VarCharType varCharType) {
        final Function<Object, byte[]> converter = varCharType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(BooleanType booleanType) {
        final Function<Object, byte[]> converter = booleanType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(BinaryType binaryType) {
        final Function<Object, byte[]> converter = binaryType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(VarBinaryType varBinaryType) {
        final Function<Object, byte[]> converter = varBinaryType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(DecimalType decimalType) {
        return o -> o == null ? 0 : getLongHash(((Decimal) o).toUnscaledLong());
    }

    @Override
    public Function<Object, Integer> visit(TinyIntType tinyIntType) {
        return o -> o == null ? 0 : getLongHash((byte) o);
    }

    @Override
    public Function<Object, Integer> visit(SmallIntType smallIntType) {
        return o -> o == null ? 0 : getLongHash((short) o);
    }

    @Override
    public Function<Object, Integer> visit(IntType intType) {
        return o -> o == null ? 0 : getLongHash((int) o);
    }

    @Override
    public Function<Object, Integer> visit(BigIntType bigIntType) {
        return o -> o == null ? 0 : getLongHash((long) o);
    }

    @Override
    public Function<Object, Integer> visit(FloatType floatType) {
        return o -> o == null ? 0 : getLongHash(Float.floatToIntBits((float) o));
    }

    @Override
    public Function<Object, Integer> visit(DoubleType doubleType) {
        return o -> o == null ? 0 : getLongHash(Double.doubleToLongBits((double) o));
    }

    @Override
    public Function<Object, Integer> visit(DateType dateType) {
        return o -> o == null ? 0 : getLongHash((int) o);
    }

    @Override
    public Function<Object, Integer> visit(TimeType timeType) {
        return o -> o == null ? 0 : getLongHash((int) o);
    }

    @Override
    public Function<Object, Integer> visit(TimestampType timestampType) {
        return o -> o == null ? 0 : getLongHash(((Timestamp) o).getMillisecond());
    }

    @Override
    public Function<Object, Integer> visit(LocalZonedTimestampType localZonedTimestampType) {
        return o -> o == null ? 0 : getLongHash(((Timestamp) o).getMillisecond());
    }

    @Override
    public Function<Object, Integer> visit(ArrayType arrayType) {
        final Function<Object, byte[]> converter = arrayType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(MultisetType multisetType) {
        final Function<Object, byte[]> converter = multisetType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(MapType mapType) {
        final Function<Object, byte[]> converter = mapType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    @Override
    public Function<Object, Integer> visit(RowType rowType) {
        final Function<Object, byte[]> converter = rowType.accept(ObjectToBytesVisitor.INSTANCE);
        return o -> MurmurHashUtils.hashBytes(converter.apply(o));
    }

    // Thomas Wang's integer hash function
    // http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
    static int getLongHash(long key) {
        key = (~key) + (key << 18); // key = (key << 18) - key - 1;
        key = key ^ (key >>> 31);
        key = key * 21; // key = (key + (key << 2)) + (key << 4);
        key = key ^ (key >>> 11);
        key = key + (key << 6);
        key = key ^ (key >>> 22);
        return (int) key;
    }
}
