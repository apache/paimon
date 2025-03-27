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
import org.apache.paimon.utils.Preconditions;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.IGNORE_RETRACT;

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

        boolean removeRecordOnRetract = options.aggregationRemoveRecordOnDelete();
        boolean fieldIgnoreRetract = options.fieldAggIgnoreRetract(fieldName);
        Preconditions.checkState(
                !(removeRecordOnRetract && fieldIgnoreRetract),
                String.format(
                        "%s and %s have conflicting behavior so should not be enabled at the same time.",
                        CoreOptions.AGGREGATION_REMOVE_RECORD_ON_DELETE,
                        FIELDS_PREFIX + "." + fieldName + "." + IGNORE_RETRACT));

        FieldAggregator fieldAggregator =
                fieldAggregatorFactory.create(fieldType, options, fieldName);
        return fieldIgnoreRetract ? new FieldIgnoreRetractAgg(fieldAggregator) : fieldAggregator;
    }
}
