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

package org.apache.paimon.fileindex;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
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

import java.util.Optional;
import java.util.function.Function;

/** Fast hash for number type. */
public class FastHashForNumber implements DataTypeVisitor<Optional<Function<Object, Long>>> {

    public static final FastHashForNumber INSTANCE = new FastHashForNumber();

    @Override
    public Optional<Function<Object, Long>> visit(CharType charType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(VarCharType varCharType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(BooleanType booleanType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(BinaryType binaryType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(VarBinaryType varBinaryType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(DecimalType decimalType) {
        return Optional.of(o -> o == null ? 0L : getLongHash(((Decimal) o).toUnscaledLong()));
    }

    @Override
    public Optional<Function<Object, Long>> visit(TinyIntType tinyIntType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((byte) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(SmallIntType smallIntType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((short) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(IntType intType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((int) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(BigIntType bigIntType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((long) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(FloatType floatType) {
        return Optional.of(o -> o == null ? 0L : getLongHash(Float.floatToIntBits((float) o)));
    }

    @Override
    public Optional<Function<Object, Long>> visit(DoubleType doubleType) {
        return Optional.of(o -> o == null ? 0L : getLongHash(Double.doubleToLongBits((double) o)));
    }

    @Override
    public Optional<Function<Object, Long>> visit(DateType dateType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((int) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(TimeType timeType) {
        return Optional.of(o -> o == null ? 0L : getLongHash((int) o));
    }

    @Override
    public Optional<Function<Object, Long>> visit(TimestampType timestampType) {
        return Optional.of(o -> o == null ? 0L : getLongHash(((Timestamp) o).getMillisecond()));
    }

    @Override
    public Optional<Function<Object, Long>> visit(LocalZonedTimestampType localZonedTimestampType) {
        return Optional.of(o -> o == null ? 0L : getLongHash(((Timestamp) o).getMillisecond()));
    }

    @Override
    public Optional<Function<Object, Long>> visit(ArrayType arrayType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(MultisetType multisetType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(MapType mapType) {
        return Optional.empty();
    }

    @Override
    public Optional<Function<Object, Long>> visit(RowType rowType) {
        return Optional.empty();
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
}
