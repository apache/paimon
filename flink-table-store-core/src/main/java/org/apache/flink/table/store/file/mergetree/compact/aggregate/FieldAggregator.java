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

package org.apache.flink.table.store.file.mergetree.compact.aggregate;

import org.apache.flink.table.store.types.DataType;

import java.io.Serializable;

/** abstract class of aggregating a field of a row. */
public abstract class FieldAggregator implements Serializable {
    protected DataType fieldType;

    public FieldAggregator(DataType dataType) {
        this.fieldType = dataType;
    }

    static FieldAggregator createFieldAggregator(
            DataType fieldType, String strAgg, boolean isPrimaryKey) {
        final FieldAggregator fieldAggregator;
        if (isPrimaryKey) {
            fieldAggregator = new FieldLastValueAgg(fieldType);
        } else {
            // ordered by type root definition
            switch (strAgg) {
                case "sum":
                    fieldAggregator = new FieldSumAgg(fieldType);
                    break;
                case "max":
                    fieldAggregator = new FieldMaxAgg(fieldType);
                    break;
                case "min":
                    fieldAggregator = new FieldMinAgg(fieldType);
                    break;
                case "last_non_null_value":
                    fieldAggregator = new FieldLastNonNullValueAgg(fieldType);
                    break;
                case "last_value":
                    fieldAggregator = new FieldLastValueAgg(fieldType);
                    break;
                case "listagg":
                    fieldAggregator = new FieldListaggAgg(fieldType);
                    break;
                case "bool_or":
                    fieldAggregator = new FieldBoolOrAgg(fieldType);
                    break;
                case "bool_and":
                    fieldAggregator = new FieldBoolAndAgg(fieldType);
                    break;
                default:
                    throw new RuntimeException(
                            "Use unsupported aggregation or spell aggregate function incorrectly!");
            }
        }
        return fieldAggregator;
    }

    abstract Object agg(Object accumulator, Object inputField);
}
