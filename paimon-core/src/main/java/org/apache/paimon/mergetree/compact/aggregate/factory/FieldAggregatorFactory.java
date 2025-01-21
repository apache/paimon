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

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.FieldIgnoreRetractAgg;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.util.List;

/** Factory for {@link FieldAggregator}. */
public interface FieldAggregatorFactory extends Factory {

    FieldAggregator create(DataType fieldType, CoreOptions options, String field);

    String identifier();

    static FieldAggregator create(
            DataType fieldType, String fieldName, String aggFuncName, CoreOptions options) {
        FieldAggregatorFactory fieldAggregatorFactory =
                FactoryUtil.discoverFactory(
                        FieldAggregator.class.getClassLoader(),
                        FieldAggregatorFactory.class,
                        aggFuncName);
        if (fieldAggregatorFactory == null) {
            throw new RuntimeException(
                    String.format(
                            "Use unsupported aggregation: %s or spell aggregate function incorrectly!",
                            aggFuncName));
        }

        FieldAggregator fieldAggregator =
                fieldAggregatorFactory.create(fieldType, options, fieldName);
        return options.fieldAggIgnoreRetract(fieldName)
                ? new FieldIgnoreRetractAgg(fieldAggregator)
                : fieldAggregator;
    }

    @Nullable
    static String getAggFuncName(String fieldName, List<String> primaryKeys, CoreOptions options) {
        if (primaryKeys.contains(fieldName)) {
            // aggregate by primary keys, so they do not aggregate
            return FieldPrimaryKeyAggFactory.NAME;
        }

        String aggFunc = options.fieldAggFunc(fieldName);
        return aggFunc == null ? options.fieldsDefaultFunc() : aggFunc;
    }
}
