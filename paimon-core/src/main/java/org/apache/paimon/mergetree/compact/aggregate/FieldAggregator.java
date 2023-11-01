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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.function.Supplier;

/** abstract class of aggregating a field of a row. */
public abstract class FieldAggregator implements Serializable {
    protected DataType fieldType;

    public FieldAggregator(DataType dataType) {
        this.fieldType = dataType;
    }

    public static FieldAggregator createFieldAggregator(
            DataType fieldType,
            @Nullable String strAgg,
            boolean ignoreRetract,
            boolean isPrimaryKey,
            Supplier<FieldAggregator> defaultAggregator) {
        FieldAggregator fieldAggregator;
        if (isPrimaryKey) {
            fieldAggregator = new FieldPrimaryKeyAgg(fieldType);
        } else {
            // If the field has no aggregate function, use last_non_null_value.
            if (strAgg == null) {
                fieldAggregator = defaultAggregator.get();
            } else {
                // ordered by type root definition
                switch (strAgg) {
                    case FieldSumAgg.NAME:
                        fieldAggregator = new FieldSumAgg(fieldType);
                        break;
                    case FieldMaxAgg.NAME:
                        fieldAggregator = new FieldMaxAgg(fieldType);
                        break;
                    case FieldMinAgg.NAME:
                        fieldAggregator = new FieldMinAgg(fieldType);
                        break;
                    case FieldLastNonNullValueAgg.NAME:
                        fieldAggregator = new FieldLastNonNullValueAgg(fieldType);
                        break;
                    case FieldLastValueAgg.NAME:
                        fieldAggregator = new FieldLastValueAgg(fieldType);
                        break;
                    case FieldListaggAgg.NAME:
                        fieldAggregator = new FieldListaggAgg(fieldType);
                        break;
                    case FieldBoolOrAgg.NAME:
                        fieldAggregator = new FieldBoolOrAgg(fieldType);
                        break;
                    case FieldBoolAndAgg.NAME:
                        fieldAggregator = new FieldBoolAndAgg(fieldType);
                        break;
                    default:
                        throw new RuntimeException(
                                "Use unsupported aggregation or spell aggregate function incorrectly!");
                }
            }
        }

        if (ignoreRetract) {
            fieldAggregator = new FieldIgnoreRetractAgg(fieldAggregator);
        }
        return fieldAggregator;
    }

    abstract String name();

    public abstract Object agg(Object accumulator, Object inputField);

    public Object retract(Object accumulator, Object retractField) {
        throw new UnsupportedOperationException(
                String.format(
                        "Aggregate function '%s' does not support retraction,"
                                + " If you allow this function to ignore retraction messages,"
                                + " you can configure 'fields.${field_name}.ignore-retract'='true'.",
                        name()));
    }
}
