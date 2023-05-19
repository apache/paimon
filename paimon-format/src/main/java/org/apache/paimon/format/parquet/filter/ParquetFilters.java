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

package org.apache.paimon.format.parquet.filter;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.TimestampType;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;

import java.io.Serializable;
import java.util.Arrays;

/** Utility class that provides helper methods to work with Parquet Filter PushDown. */
public class ParquetFilters {

    /** A filter predicate that can be convert to FilterPredicate. */
    public abstract static class Predicate implements Serializable {
        public abstract FilterPredicate toParquetPredicate();
    }

    abstract static class ColumnPredicate extends ParquetFilters.Predicate {
        final String columnName;
        final DataType literalType;

        ColumnPredicate(String columnName, DataType literalType) {
            this.columnName = columnName;
            this.literalType = literalType;
        }
    }

    abstract static class BinaryPredicate extends ParquetFilters.ColumnPredicate {
        final Serializable literal;

        BinaryPredicate(String columnName, DataType literalType, Serializable literal) {
            super(columnName, literalType);
            this.literal = literal;
        }
    }

    /** An EQUALS predicate that can be evaluated by the Parquet Reader. */
    public static class Equals extends ParquetFilters.BinaryPredicate {
        /**
         * Creates an EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public Equals(String columnName, DataType literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            return predicate(this, literalType, columnName, literal);
        }

        @Override
        public String toString() {
            return columnName + " = " + literal;
        }
    }

    /** An LessThan predicate that can be evaluated by the Parquet Reader. */
    public static class LessThan extends ParquetFilters.BinaryPredicate {
        /**
         * Creates a LESS_THAN predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThan(String columnName, DataType literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            return predicate(this, literalType, columnName, literal);
        }

        @Override
        public String toString() {
            return columnName + " < " + literal;
        }
    }

    /** An LessThanEquals predicate that can be evaluated by the Parquet Reader. */
    public static class LessThanEquals extends ParquetFilters.BinaryPredicate {
        /**
         * Creates a LESS_THAN_EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThanEquals(String columnName, DataType literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            return predicate(this, literalType, columnName, literal);
        }

        @Override
        public String toString() {
            return columnName + " <= " + literal;
        }
    }

    /** An IS_NULL predicate that can be evaluated by the Parquet Reader. */
    public static class IsNull extends ParquetFilters.ColumnPredicate {
        /**
         * Creates an IS_NULL predicate.
         *
         * @param columnName The column to check for null.
         * @param literalType The type of the column to check for null.
         */
        public IsNull(String columnName, DataType literalType) {
            super(columnName, literalType);
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            return predicate(this, literalType, columnName, null);
        }

        @Override
        public String toString() {
            return columnName + " IS NULL";
        }
    }

    /** An Not predicate that can be evaluated by the Parquet Reader. */
    public static class Not extends ParquetFilters.Predicate {
        private final ParquetFilters.Predicate pred;

        /**
         * Creates a NOT predicate.
         *
         * @param predicate The predicate to negate.
         */
        public Not(ParquetFilters.Predicate predicate) {
            this.pred = predicate;
        }

        public FilterPredicate toParquetPredicate() {
            return FilterApi.not(pred.toParquetPredicate());
        }

        @Override
        public String toString() {
            return "NOT(" + pred.toString() + ")";
        }
    }

    /** An And predicate that can be evaluated by the Parquet Reader. */
    public static class And extends ParquetFilters.Predicate {
        private final ParquetFilters.Predicate[] preds;

        /**
         * Creates an AND predicate.
         *
         * @param predicates The disjunctive predicates.
         */
        public And(ParquetFilters.Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            if (preds.length != 2) {
                throw new RuntimeException("Illegal and children: " + preds.length);
            }

            return FilterApi.and(preds[0].toParquetPredicate(), preds[1].toParquetPredicate());
        }

        @Override
        public String toString() {
            return "AND(" + Arrays.toString(preds) + ")";
        }
    }

    /** An Or predicate that can be evaluated by the Parquet Reader. */
    public static class Or extends ParquetFilters.Predicate {
        private final ParquetFilters.Predicate[] preds;

        /**
         * Creates an OR predicate.
         *
         * @param predicates The disjunctive predicates.
         */
        public Or(ParquetFilters.Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public String toString() {
            return "OR(" + Arrays.toString(preds) + ")";
        }

        @Override
        public FilterPredicate toParquetPredicate() {
            if (preds.length != 2) {
                throw new RuntimeException("Illegal and children: " + preds.length);
            }

            return FilterApi.or(preds[0].toParquetPredicate(), preds[1].toParquetPredicate());
        }
    }

    private static FilterPredicate predicate(
            ParquetFilters.Predicate predicate,
            DataType dataType,
            String name,
            Serializable literal) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                Operators.BooleanColumn col = FilterApi.booleanColumn(name);
                if (predicate instanceof Equals) {
                    return FilterApi.eq(col, toParquetObject(dataType, literal));
                } else if (predicate instanceof Not) {
                    return FilterApi.eq(col, toParquetObject(dataType, literal));
                }
                break;
            case INTEGER:
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
                Operators.IntColumn intColumn = FilterApi.intColumn(name);
                return pred(predicate, intColumn, toParquetObject(dataType, literal));
            case FLOAT:
                return pred(
                        predicate, FilterApi.floatColumn(name), toParquetObject(dataType, literal));
            case DOUBLE:
                return pred(
                        predicate,
                        FilterApi.doubleColumn(name),
                        toParquetObject(dataType, literal));
            case BIGINT:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return geLongtFilterPredicate(predicate, dataType, name, literal);
            case BINARY:
            case VARCHAR:
            case CHAR:
            case VARBINARY:
                return getBinaryFilterPredicate(predicate, dataType, name, literal);
        }
        return null;
    }

    private static FilterPredicate getBinaryFilterPredicate(
            Predicate predicate, DataType dataType, String name, Serializable literal) {
        Operators.BinaryColumn binaryColumn = FilterApi.binaryColumn(name);
        return pred(predicate, binaryColumn, toParquetObject(dataType, literal));
    }

    private static FilterPredicate geLongtFilterPredicate(
            Predicate predicate, DataType dataType, String name, Serializable literal) {
        Operators.LongColumn longColumn = FilterApi.longColumn(name);
        return pred(predicate, longColumn, toParquetObject(dataType, literal));
    }

    private static <
                    C extends Comparable<C>,
                    COL extends Operators.Column<C> & Operators.SupportsLtGt>
            FilterPredicate pred(ParquetFilters.Predicate predicate, COL col, C value) {
        if (predicate instanceof Equals) {
            return FilterApi.eq(col, value);
        } else if (predicate instanceof LessThan) {
            return FilterApi.lt(col, value);
        } else if (predicate instanceof LessThanEquals) {
            return FilterApi.ltEq(col, value);
        } else if (predicate instanceof IsNull) {
            return FilterApi.eq(col, value);
        } else {
            return null;
        }
    }

    private static <C extends Comparable<C>> C toParquetObject(DataType type, Object literal) {
        if (null == literal) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARCHAR:
            case CHAR:
                return (C) Binary.fromString(literal.toString());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                int precision = timestampType.getPrecision();
                Long timestamp;
                if (precision <= 3) {
                    timestamp = ((Timestamp) literal).getMillisecond();
                } else {
                    timestamp = ((Timestamp) literal).toMicros();
                }
                return (C) timestamp;
        }

        return (C) literal;
    }
}
