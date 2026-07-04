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
import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.List;

import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.NESTED_KEY_NULL_STRATEGY;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory for #{@link FieldNestedUpdateAgg}. */
public class FieldNestedUpdateAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "nested_update";

    @Override
    public FieldNestedUpdateAgg create(DataType fieldType, CoreOptions options, String field) {
        String typeErrorMsg =
                "Data type for nested table column must be 'Array<Row>' but was '%s'.";
        checkArgument(fieldType instanceof ArrayType, typeErrorMsg, fieldType);
        ArrayType arrayType = (ArrayType) fieldType;
        checkArgument(arrayType.getElementType() instanceof RowType, typeErrorMsg, fieldType);

        checkOptionDependencies(options, field);

        return new FieldNestedUpdateAgg(
                identifier(),
                arrayType,
                options.fieldNestedUpdateAggNestedKey(field),
                options.fieldNestedUpdateAggNestedKeyNullStrategy(field),
                options.fieldNestedUpdateAggNestedSequenceField(field),
                options.fieldNestedUpdateAggCountLimit(field));
    }

    @Override
    public String identifier() {
        return NAME;
    }

    private void checkOptionDependencies(CoreOptions options, String field) {
        List<String> nestedKey = options.fieldNestedUpdateAggNestedKey(field);

        boolean strategyConfigured =
                options.toConfiguration()
                        .containsKey(FIELDS_PREFIX + "." + field + "." + NESTED_KEY_NULL_STRATEGY);

        checkArgument(
                !strategyConfigured || !nestedKey.isEmpty(),
                "Option 'fields.<field-name>.nested-key-null-strategy' requires "
                        + "'fields.<field-name>.nested-key' to be configured.");

        checkArgument(
                options.fieldNestedUpdateAggNestedSequenceField(field).isEmpty()
                        || !nestedKey.isEmpty(),
                "Option 'fields.<field-name>.nested-sequence-field' requires "
                        + "'fields.<field-name>.nested-key' to be configured.");
    }
}
