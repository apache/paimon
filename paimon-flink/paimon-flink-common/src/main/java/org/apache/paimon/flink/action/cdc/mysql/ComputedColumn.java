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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A Computed column's value is computed from input columns. Only expression with at most two inputs
 * (with referenced field at the first) is supported currently.
 */
public class ComputedColumn implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final Expression expression;

    public ComputedColumn(
            String columnName, String exprName, Map<String, DataType> typeMapping, String[] args) {
        checkArgument(
                args.length >= 1 && args.length <= 2,
                "Currently, computed column only supports one or two arguments.");
        String fieldReference = args[0];
        String literal = args.length == 2 ? args[1] : null;
        checkArgument(
                typeMapping.containsKey(fieldReference),
                String.format(
                        "Referenced field '%s' is not in given MySQL fields: %s.",
                        fieldReference, typeMapping.keySet()));
        this.columnName = columnName;
        this.expression =
                Expression.create(
                        exprName, fieldReference, typeMapping.get(fieldReference), literal);
    }

    public String columnName() {
        return columnName;
    }

    public DataType columnType() {
        return expression.outputType();
    }

    String fieldReference() {
        return expression.fieldReference();
    }

    /** Compute column's value from given argument. Return null if input is null. */
    @Nullable
    String eval(@Nullable String input) {
        if (input == null) {
            return null;
        }
        return expression.eval(input);
    }
}
