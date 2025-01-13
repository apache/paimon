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
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory for #{@link FieldNestedUpdateAgg}. */
public class FieldNestedUpdateAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "nested_update";

    @Override
    public FieldAggregator create(DataType fieldType, CoreOptions options, String field) {
        return createFieldNestedUpdateAgg(fieldType, options.fieldNestedUpdateAggNestedKey(field));
    }

    @Override
    public String identifier() {
        return NAME;
    }

    private FieldAggregator createFieldNestedUpdateAgg(DataType fieldType, List<String> nestedKey) {
        if (nestedKey == null) {
            nestedKey = Collections.emptyList();
        }

        String typeErrorMsg =
                "Data type for nested table column must be 'Array<Row>' but was '%s'.";
        checkArgument(fieldType instanceof ArrayType, typeErrorMsg, fieldType);
        ArrayType arrayType = (ArrayType) fieldType;
        checkArgument(arrayType.getElementType() instanceof RowType, typeErrorMsg, fieldType);

        return new FieldNestedUpdateAgg(identifier(), arrayType, nestedKey);
    }
}
