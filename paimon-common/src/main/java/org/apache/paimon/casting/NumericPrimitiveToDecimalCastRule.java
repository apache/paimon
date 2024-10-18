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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DecimalUtils;

/**
 * {@link DataTypeFamily#INTEGER_NUMERIC} and {@link DataTypeFamily#APPROXIMATE_NUMERIC} to {@link
 * DataTypeRoot#DECIMAL} cast rule.
 */
class NumericPrimitiveToDecimalCastRule extends AbstractCastRule<Number, Decimal> {

    static final NumericPrimitiveToDecimalCastRule INSTANCE =
            new NumericPrimitiveToDecimalCastRule();

    private NumericPrimitiveToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.INTEGER_NUMERIC)
                        .input(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .target(DataTypeRoot.DECIMAL)
                        .build());
    }

    @Override
    public CastExecutor<Number, Decimal> create(DataType inputType, DataType targetType) {
        final DecimalType decimalType = (DecimalType) targetType;
        if (inputType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return value ->
                    DecimalUtils.castFrom(
                            value.longValue(), decimalType.getPrecision(), decimalType.getScale());
        }
        return value ->
                DecimalUtils.castFrom(
                        value.doubleValue(), decimalType.getPrecision(), decimalType.getScale());
    }
}
