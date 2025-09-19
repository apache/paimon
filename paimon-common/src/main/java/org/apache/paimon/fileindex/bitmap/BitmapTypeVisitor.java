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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
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
import org.apache.paimon.types.VariantType;

/** Simplified visitor for bitmap index. */
public abstract class BitmapTypeVisitor<R> implements DataTypeVisitor<R> {

    public abstract R visitBinaryString();

    public abstract R visitByte();

    public abstract R visitShort();

    public abstract R visitInt();

    public abstract R visitLong();

    public abstract R visitFloat();

    public abstract R visitDouble();

    public abstract R visitBoolean();

    @Override
    public final R visit(CharType charType) {
        return visitBinaryString();
    }

    @Override
    public final R visit(VarCharType varCharType) {
        return visitBinaryString();
    }

    @Override
    public final R visit(BooleanType booleanType) {
        return visitBoolean();
    }

    @Override
    public final R visit(BinaryType binaryType) {
        throw new UnsupportedOperationException("Does not support type binary");
    }

    @Override
    public final R visit(VarBinaryType varBinaryType) {
        throw new UnsupportedOperationException("Does not support type binary");
    }

    @Override
    public final R visit(DecimalType decimalType) {
        throw new UnsupportedOperationException("Does not support decimal");
    }

    @Override
    public final R visit(TinyIntType tinyIntType) {
        return visitByte();
    }

    @Override
    public final R visit(SmallIntType smallIntType) {
        return visitShort();
    }

    @Override
    public final R visit(IntType intType) {
        return visitInt();
    }

    @Override
    public final R visit(BigIntType bigIntType) {
        return visitLong();
    }

    @Override
    public final R visit(FloatType floatType) {
        return visitFloat();
    }

    @Override
    public final R visit(DoubleType doubleType) {
        return visitDouble();
    }

    @Override
    public final R visit(DateType dateType) {
        return visitInt();
    }

    @Override
    public final R visit(TimeType timeType) {
        return visitInt();
    }

    @Override
    public R visit(TimestampType timestampType) {
        return visitLong();
    }

    @Override
    public R visit(LocalZonedTimestampType localZonedTimestampType) {
        return visitLong();
    }

    @Override
    public final R visit(ArrayType arrayType) {
        throw new UnsupportedOperationException("Does not support type array");
    }

    @Override
    public final R visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("Does not support type mutiset");
    }

    @Override
    public final R visit(MapType mapType) {
        throw new UnsupportedOperationException("Does not support type map");
    }

    @Override
    public final R visit(RowType rowType) {
        throw new UnsupportedOperationException("Does not support type row");
    }

    @Override
    public final R visit(VariantType rowType) {
        throw new UnsupportedOperationException("Does not support type variant");
    }

    @Override
    public final R visit(BlobType blobType) {
        throw new UnsupportedOperationException("Does not support type blob");
    }
}
