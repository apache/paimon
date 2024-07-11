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

package org.apache.parquet.filter2.predicate;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.Predicate;
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

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.io.api.Binary;

import java.util.List;

/** Convert {@link Predicate} to {@link FilterCompat.Filter}. */
public class ParquetFilters {

    private static final ConvertFilterToParquet CONVERTER = new ConvertFilterToParquet();

    private ParquetFilters() {}

    public static FilterCompat.Filter convert(List<Predicate> predicates) {
        FilterPredicate result = null;
        if (predicates != null) {
            for (Predicate predicate : predicates) {
                try {
                    FilterPredicate parquetFilter = predicate.visit(CONVERTER);
                    if (result == null) {
                        result = parquetFilter;
                    } else {
                        result = FilterApi.and(result, parquetFilter);
                    }
                } catch (UnsupportedOperationException ignore) {
                }
            }
        }

        return result != null ? FilterCompat.get(result) : FilterCompat.NOOP;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class ConvertFilterToParquet implements FunctionVisitor<FilterPredicate> {

        @Override
        public FilterPredicate visitIsNotNull(FieldRef fieldRef) {
            return new Operators.NotEq<>(toParquetColumn(fieldRef), null);
        }

        @Override
        public FilterPredicate visitIsNull(FieldRef fieldRef) {
            return new Operators.Eq<>(toParquetColumn(fieldRef), null);
        }

        @Override
        public FilterPredicate visitLessThan(FieldRef fieldRef, Object literal) {
            return new Operators.Lt(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new Operators.GtEq(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitNotEqual(FieldRef fieldRef, Object literal) {
            return new Operators.NotEq(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new Operators.LtEq(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitEqual(FieldRef fieldRef, Object literal) {
            return new Operators.Eq(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new Operators.Gt(toParquetColumn(fieldRef), toParquetObject(literal));
        }

        @Override
        public FilterPredicate visitAnd(List<FilterPredicate> children) {
            if (children.size() != 2) {
                throw new RuntimeException("Illegal and children: " + children.size());
            }

            return FilterApi.and(children.get(0), children.get(1));
        }

        @Override
        public FilterPredicate visitOr(List<FilterPredicate> children) {
            if (children.size() != 2) {
                throw new RuntimeException("Illegal and children: " + children.size());
            }

            return FilterApi.or(children.get(0), children.get(1));
        }

        @Override
        public FilterPredicate visitStartsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitEndsWith(FieldRef fieldRef, Object literal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitIn(FieldRef fieldRef, List<Object> literals) {
            throw new UnsupportedOperationException();
        }

        @Override
        public FilterPredicate visitNotIn(FieldRef fieldRef, List<Object> literals) {
            throw new UnsupportedOperationException();
        }
    }

    private static Operators.Column<?> toParquetColumn(FieldRef fieldRef) {
        return fieldRef.type().accept(new ConvertToColumnTypeVisitor(fieldRef.name()));
    }

    private static Comparable<?> toParquetObject(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return (Comparable<?>) value;
        } else if (value instanceof String) {
            return Binary.fromString((String) value);
        } else if (value instanceof BinaryString) {
            return Binary.fromString(value.toString());
        } else if (value instanceof byte[]) {
            return Binary.fromReusedByteArray((byte[]) value);
        }

        // TODO Support Decimal and Timestamp
        throw new UnsupportedOperationException();
    }

    private static class ConvertToColumnTypeVisitor
            implements DataTypeVisitor<Operators.Column<?>> {

        private final String name;

        public ConvertToColumnTypeVisitor(String name) {
            this.name = name;
        }

        @Override
        public Operators.Column<?> visit(CharType charType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(VarCharType varCharType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BooleanType booleanType) {
            return FilterApi.booleanColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BinaryType binaryType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(VarBinaryType varBinaryType) {
            return FilterApi.binaryColumn(name);
        }

        @Override
        public Operators.Column<?> visit(TinyIntType tinyIntType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(SmallIntType smallIntType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(IntType intType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(BigIntType bigIntType) {
            return FilterApi.longColumn(name);
        }

        @Override
        public Operators.Column<?> visit(FloatType floatType) {
            return FilterApi.floatColumn(name);
        }

        @Override
        public Operators.Column<?> visit(DoubleType doubleType) {
            return FilterApi.doubleColumn(name);
        }

        @Override
        public Operators.Column<?> visit(DateType dateType) {
            return FilterApi.intColumn(name);
        }

        @Override
        public Operators.Column<?> visit(TimeType timeType) {
            return FilterApi.intColumn(name);
        }

        // TODO we can support decimal and timestamp

        @Override
        public Operators.Column<?> visit(DecimalType decimalType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(TimestampType timestampType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(LocalZonedTimestampType localZonedTimestampType) {
            throw new UnsupportedOperationException();
        }

        // ===================== can not support =========================

        @Override
        public Operators.Column<?> visit(ArrayType arrayType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(MultisetType multisetType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(MapType mapType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operators.Column<?> visit(RowType rowType) {
            throw new UnsupportedOperationException();
        }
    }
}
