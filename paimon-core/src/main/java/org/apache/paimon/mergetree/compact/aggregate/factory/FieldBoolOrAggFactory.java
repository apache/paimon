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
import org.apache.paimon.mergetree.compact.aggregate.FieldBoolOrAgg;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory for #{@link FieldBoolOrAgg}. */
public class FieldBoolOrAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "bool_or";

    @Override
    public FieldBoolOrAgg create(DataType fieldType, CoreOptions options, String field) {
        checkArgument(
                fieldType instanceof BooleanType,
                "Data type for bool or column must be 'BooleanType' but was '%s'.",
                fieldType);
        return new FieldBoolOrAgg(identifier(), (BooleanType) fieldType);
    }

    @Override
    public String identifier() {
        return NAME;
    }
}
