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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Produce a computation result for computed column. */
public interface Expression extends Serializable {

    List<String> SUPPORTED_EXPRESSION = Collections.singletonList("year");

    /** Return name of referenced field. */
    String fieldReference();

    /** Return {@link DataType} of computed value. */
    DataType outputType();

    /** Compute value from given input. Input and output are serialized to string. */
    String eval(String input);

    static Expression create(
            String exprName, String fieldReference, DataType fieldType, String... literals) {
        switch (exprName) {
            case "year":
                return year(fieldReference);
            case "substring":
                return substring(fieldReference, literals);
                // TODO: support more expression
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported expression: %s. Supported expressions are: %s",
                                exprName, String.join(",", SUPPORTED_EXPRESSION)));
        }
    }

    static Expression year(String fieldReference) {
        return new YearComputer(fieldReference);
    }

    static Expression substring(String fieldReference, String... literals) {
        checkArgument(
                literals.length == 2,
                "'substring' expression needs begin index and end index arguments.");
        int beginInclusive, endExclusive;
        try {
            beginInclusive = Integer.parseInt(literals[0]);
            endExclusive = Integer.parseInt(literals[1]);
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    String.format(
                            "begin index (%s) or end index (%s) is not an integer.",
                            literals[0], literals[1]),
                    e);
        }
        checkArgument(
                beginInclusive >= 0,
                "begin index argument (%s) of 'substring' must be >= 0.",
                beginInclusive);
        checkArgument(
                endExclusive > beginInclusive,
                "end index (%s) must be larger than begin index (%s).",
                endExclusive,
                beginInclusive);
        return new Substring(fieldReference, beginInclusive, endExclusive);
    }

    /** Expression that only reference single field. */
    abstract class SingleFieldReferenceExpression implements Expression {
        private final String fieldReference;

        private SingleFieldReferenceExpression(String fieldReference) {
            this.fieldReference = fieldReference;
        }

        @Override
        public String fieldReference() {
            return fieldReference;
        }
    }

    /** Compute year from a time input. */
    final class YearComputer extends SingleFieldReferenceExpression {

        private static final long serialVersionUID = 1L;

        private YearComputer(String fieldReference) {
            super(fieldReference);
        }

        @Override
        public DataType outputType() {
            return DataTypes.INT();
        }

        @Override
        public String eval(String input) {
            LocalDateTime localDateTime = DateTimeUtils.toLocalDateTime(input, 0);
            return String.valueOf(localDateTime.getYear());
        }
    }

    /** Get substring using {@link String#substring(int, int)}. */
    final class Substring extends SingleFieldReferenceExpression {

        private static final long serialVersionUID = 1L;

        private final int beginInclusive;
        private final int endExclusive;

        private Substring(String fieldReference, int beginInclusive, int endExclusive) {
            super(fieldReference);
            this.beginInclusive = beginInclusive;
            this.endExclusive = endExclusive;
        }

        @Override
        public DataType outputType() {
            return DataTypes.VARCHAR(endExclusive - beginInclusive);
        }

        @Override
        public String eval(String input) {
            if (endExclusive > input.length()) {
                throw new RuntimeException(
                        String.format(
                                "Cannot get substring from '%s' because the end index '%s' is out of range.",
                                input, endExclusive));
            }
            return input.substring(beginInclusive, endExclusive);
        }
    }
}
