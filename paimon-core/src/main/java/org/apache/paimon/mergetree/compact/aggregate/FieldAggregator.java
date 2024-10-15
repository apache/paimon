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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

/** abstract class of aggregating a field of a row. */
public abstract class FieldAggregator implements Serializable {
    protected DataType fieldType;

    private static final long serialVersionUID = 1L;

    public FieldAggregator(DataType dataType) {
        this.fieldType = dataType;
    }

    public static FieldAggregator createFieldAggregator(
            DataType fieldType,
            @Nullable String strAgg,
            boolean ignoreRetract,
            boolean isPrimaryKey,
            CoreOptions options,
            String field) {
        FieldAggregator fieldAggregator;
        if (isPrimaryKey) {
            strAgg = FieldPrimaryKeyAgg.NAME;
        } else if (strAgg == null) {
            strAgg = FieldLastNonNullValueAgg.NAME;
        }

        FieldAggregatorFactory fieldAggregatorFactory =
                FactoryUtil.discoverFactory(
                        FieldAggregator.class.getClassLoader(),
                        FieldAggregatorFactory.class,
                        strAgg);
        if (fieldAggregatorFactory == null) {
            throw new RuntimeException(
                    String.format(
                            "Use unsupported aggregation: %s or spell aggregate function incorrectly!",
                            strAgg));
        }

        fieldAggregator = fieldAggregatorFactory.create(fieldType, options, field);

        if (ignoreRetract) {
            fieldAggregator = new FieldIgnoreRetractAgg(fieldAggregator);
        }

        return fieldAggregator;
    }

    public abstract String name();

    public abstract Object agg(Object accumulator, Object inputField);

    public Object aggReversed(Object accumulator, Object inputField) {
        return agg(inputField, accumulator);
    }

    /** reset the aggregator to a clean start state. */
    public void reset() {}

    public Object retract(Object accumulator, Object retractField) {
        throw new UnsupportedOperationException(
                String.format(
                        "Aggregate function '%s' does not support retraction,"
                                + " If you allow this function to ignore retraction messages,"
                                + " you can configure 'fields.${field_name}.ignore-retract'='true'.",
                        name()));
    }
}
