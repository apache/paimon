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
import org.apache.paimon.mergetree.compact.aggregate.FieldListaggAgg;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory for #{@link FieldListaggAgg}. */
public class FieldListaggAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "listagg";

    @Override
    public FieldListaggAgg create(DataType fieldType, CoreOptions options, String field) {
        checkArgument(
                fieldType instanceof VarCharType,
                "Data type for list agg column must be 'VarCharType' but was '%s'.",
                fieldType);
        return new FieldListaggAgg(identifier(), (VarCharType) fieldType, options, field);
    }

    @Override
    public String identifier() {
        return NAME;
    }
}
