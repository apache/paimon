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

package org.apache.paimon.casting;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;

/** {@link DataTypeFamily#INTEGER_NUMERIC} to {@link DataTypeRoot#BOOLEAN} conversions. */
class NumericToBooleanCastRule extends AbstractCastRule<Number, Boolean> {

    static final NumericToBooleanCastRule INSTANCE = new NumericToBooleanCastRule();

    private NumericToBooleanCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeRoot.BOOLEAN)
                        .build());
    }

    @Override
    public CastExecutor<Number, Boolean> create(DataType inputType, DataType targetType) {
        return value -> value.intValue() != 0;
    }
}
