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
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compute value for computed column. */
public interface ColumnValueComputer extends Serializable {

    List<String> SUPPORTED_EXPRESSION = Collections.singletonList("year");

    /** Return input column name. */
    String inputName();

    /** Return {@link DataType} of computed value. */
    DataType outputType();

    /** Compute value from given input. Input and output are serialize to string. */
    String computeValue(String input);

    static ColumnValueComputer create(
            String exprName, Map<String, DataType> typeMapping, String[] args) {
        switch (exprName) {
            case "year":
                return year(args, typeMapping);
                // TODO: support more expression
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported expression: %s. Supported expressions are: %s",
                                exprName, String.join(",", SUPPORTED_EXPRESSION)));
        }
    }

    static ColumnValueComputer year(String[] args, Map<String, DataType> typeMapping) {
        checkArgument(args.length == 1, "'year' computer only support single argument.");
        checkArgument(
                typeMapping.containsKey(args[0]),
                String.format(
                        "Input column '%s' is not in given fields: %s.",
                        args[0], typeMapping.keySet()));
        return new YearComputer(args[0]);
    }

    /** Compute year from a time input. */
    final class YearComputer implements ColumnValueComputer {

        private static final long serialVersionUID = 1L;

        private final String inputName;

        private YearComputer(String inputName) {
            this.inputName = inputName;
        }

        @Override
        public String inputName() {
            return inputName;
        }

        @Override
        public DataType outputType() {
            return DataTypes.INT();
        }

        @Override
        public String computeValue(String input) {
            LocalDateTime localDateTime = DateTimeUtils.toLocalDateTime(input);
            return String.valueOf(localDateTime.getYear());
        }
    }
}
