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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * The visitor definition of {@link DataType}. The visitor transforms a data type into instances of
 * {@code R}.
 *
 * @param <R> result type
 * @since 0.4.0
 */
@Public
public interface DataTypeVisitor<R> {

    R visit(CharType charType);

    R visit(VarCharType varCharType);

    R visit(BooleanType booleanType);

    R visit(BinaryType binaryType);

    R visit(VarBinaryType varBinaryType);

    R visit(DecimalType decimalType);

    R visit(TinyIntType tinyIntType);

    R visit(SmallIntType smallIntType);

    R visit(IntType intType);

    R visit(BigIntType bigIntType);

    R visit(FloatType floatType);

    R visit(DoubleType doubleType);

    R visit(DateType dateType);

    R visit(TimeType timeType);

    R visit(TimestampType timestampType);

    R visit(LocalZonedTimestampType localZonedTimestampType);

    R visit(VariantType variantType);

    R visit(ArrayType arrayType);

    R visit(MultisetType multisetType);

    R visit(MapType mapType);

    R visit(RowType rowType);
}
