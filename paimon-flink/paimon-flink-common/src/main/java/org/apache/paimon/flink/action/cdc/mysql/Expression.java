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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

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
            String exprName, String fieldReference, DataType fieldType, @Nullable String literal) {
        switch (exprName) {
            case "year":
                return year(fieldReference);
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

    /** Compute year from a time input. */
    final class YearComputer implements Expression {

        private static final long serialVersionUID = 1L;

        private final String fieldReference;

        private YearComputer(String fieldReference) {
            this.fieldReference = fieldReference;
        }

        @Override
        public String fieldReference() {
            return fieldReference;
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
}
