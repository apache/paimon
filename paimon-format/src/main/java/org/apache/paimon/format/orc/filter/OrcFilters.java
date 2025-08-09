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

package org.apache.paimon.format.orc.filter;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

/** Utility class that provides helper methods to work with Orc Filter PushDown. */
public class OrcFilters {

    // --------------------------------------------------------------------------------------------
    //  Classes to define predicates
    // --------------------------------------------------------------------------------------------

    /** A filter predicate that can be evaluated by the OrcInputFormat. */
    public abstract static class Predicate implements Serializable {
        public abstract SearchArgument.Builder add(SearchArgument.Builder builder);
    }

    abstract static class ColumnPredicate extends Predicate {
        final String columnName;
        final PredicateLeaf.Type literalType;

        ColumnPredicate(String columnName, PredicateLeaf.Type literalType) {
            this.columnName = columnName;
            this.literalType = literalType;
        }

        Object castLiteral(Serializable literal) {

            switch (literalType) {
                case LONG:
                    if (literal instanceof Byte) {
                        return new Long((Byte) literal);
                    } else if (literal instanceof Short) {
                        return new Long((Short) literal);
                    } else if (literal instanceof Integer) {
                        return new Long((Integer) literal);
                    } else if (literal instanceof Long) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a LONG column requires an integer "
                                        + "literal, i.e., Byte, Short, Integer, or Long.");
                    }
                case FLOAT:
                    if (literal instanceof Float) {
                        return new Double((Float) literal);
                    } else if (literal instanceof Double) {
                        return literal;
                    } else if (literal instanceof BigDecimal) {
                        return ((BigDecimal) literal).doubleValue();
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a FLOAT column requires a floating "
                                        + "literal, i.e., Float or Double.");
                    }
                case STRING:
                    if (literal instanceof String) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a STRING column requires a floating "
                                        + "literal, i.e., Float or Double.");
                    }
                case BOOLEAN:
                    if (literal instanceof Boolean) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a BOOLEAN column requires a Boolean literal.");
                    }
                case DATE:
                    if (literal instanceof Date) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a DATE column requires a java.sql.Date literal.");
                    }
                case TIMESTAMP:
                    if (literal instanceof Timestamp) {
                        return literal;
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a TIMESTAMP column requires a java.sql.Timestamp literal.");
                    }
                case DECIMAL:
                    if (literal instanceof BigDecimal) {
                        return new HiveDecimalWritable(HiveDecimal.create((BigDecimal) literal));
                    } else {
                        throw new IllegalArgumentException(
                                "A predicate on a DECIMAL column requires a BigDecimal literal.");
                    }
                default:
                    throw new IllegalArgumentException("Unknown literal type " + literalType);
            }
        }
    }

    abstract static class BinaryPredicate extends ColumnPredicate {
        final Serializable literal;

        BinaryPredicate(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType);
            this.literal = literal;
        }
    }

    /** An EQUALS predicate that can be evaluated by the OrcInputFormat. */
    public static class Equals extends BinaryPredicate {
        /**
         * Creates an EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public Equals(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.equals(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " = " + literal;
        }
    }

    /** A LESS_THAN predicate that can be evaluated by the OrcInputFormat. */
    public static class LessThan extends BinaryPredicate {
        /**
         * Creates a LESS_THAN predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThan(String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.lessThan(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " < " + literal;
        }
    }

    /** A LESS_THAN_EQUALS predicate that can be evaluated by the OrcInputFormat. */
    public static class LessThanEquals extends BinaryPredicate {
        /**
         * Creates a LESS_THAN_EQUALS predicate.
         *
         * @param columnName The column to check.
         * @param literalType The type of the literal.
         * @param literal The literal value to check the column against.
         */
        public LessThanEquals(
                String columnName, PredicateLeaf.Type literalType, Serializable literal) {
            super(columnName, literalType, literal);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.lessThanEquals(columnName, literalType, castLiteral(literal));
        }

        @Override
        public String toString() {
            return columnName + " <= " + literal;
        }
    }

    /** An IS_NULL predicate that can be evaluated by the OrcInputFormat. */
    public static class IsNull extends ColumnPredicate {
        /**
         * Creates an IS_NULL predicate.
         *
         * @param columnName The column to check for null.
         * @param literalType The type of the column to check for null.
         */
        public IsNull(String columnName, PredicateLeaf.Type literalType) {
            super(columnName, literalType);
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return builder.isNull(columnName, literalType);
        }

        @Override
        public String toString() {
            return columnName + " IS NULL";
        }
    }

    /** A NOT predicate to negate a predicate that can be evaluated by the OrcInputFormat. */
    public static class Not extends Predicate {
        private final Predicate pred;

        /**
         * Creates a NOT predicate.
         *
         * @param predicate The predicate to negate.
         */
        public Not(Predicate predicate) {
            this.pred = predicate;
        }

        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            return pred.add(builder.startNot()).end();
        }

        @Override
        public String toString() {
            return "NOT(" + pred.toString() + ")";
        }
    }

    /** An OR predicate that can be evaluated by the OrcInputFormat. */
    public static class Or extends Predicate {
        private final Predicate[] preds;

        /**
         * Creates an OR predicate.
         *
         * @param predicates The disjunctive predicates.
         */
        public Or(Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            SearchArgument.Builder withOr = builder.startOr();
            for (Predicate p : preds) {
                withOr = p.add(withOr);
            }
            return withOr.end();
        }

        @Override
        public String toString() {
            return "OR(" + Arrays.toString(preds) + ")";
        }
    }

    /** An AND predicate that can be evaluated by the OrcInputFormat. */
    public static class And extends Predicate {
        private final Predicate[] preds;

        /**
         * Creates an AND predicate.
         *
         * @param predicates The disjunctive predicates.
         */
        public And(Predicate... predicates) {
            this.preds = predicates;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            SearchArgument.Builder withAnd = builder.startAnd();
            for (Predicate pred : preds) {
                withAnd = pred.add(withAnd);
            }
            return withAnd.end();
        }

        @Override
        public String toString() {
            return "AND(" + Arrays.toString(preds) + ")";
        }
    }

    /** An IN predicate that can be evaluated by the OrcInputFormat. */
    public static class In extends ColumnPredicate {
        private final Object[] literals;

        /** Creates an IN predicate. */
        public In(String columnName, PredicateLeaf.Type literalType, Object... literals) {
            super(columnName, literalType);
            this.literals = literals;
        }

        @Override
        public SearchArgument.Builder add(SearchArgument.Builder builder) {
            Object[] castedLiterals = new Object[literals.length];
            for (int i = 0; i < literals.length; i++) {
                if (literals[i] instanceof Serializable) {
                    castedLiterals[i] = castLiteral((Serializable) literals[i]);
                } else {
                    castedLiterals[i] = literals[i];
                }
            }
            return builder.in(columnName, literalType, castedLiterals);
        }

        @Override
        public String toString() {
            return columnName + " IN " + Arrays.toString(literals);
        }
    }
}
