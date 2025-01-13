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
import org.apache.paimon.utils.DecimalUtils;

/**
 * {@link DataTypeRoot#DECIMAL} to {@link DataTypeFamily#INTEGER_NUMERIC} and {@link
 * DataTypeFamily#APPROXIMATE_NUMERIC} cast rule.
 */
class DecimalToNumericPrimitiveCastRule extends AbstractCastRule<Decimal, Number> {

    static final DecimalToNumericPrimitiveCastRule INSTANCE =
            new DecimalToNumericPrimitiveCastRule();

    private DecimalToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.DECIMAL)
                        .target(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    @Override
    public CastExecutor<Decimal, Number> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return value -> (byte) DecimalUtils.castToIntegral(value);
            case SMALLINT:
                return value -> (short) DecimalUtils.castToIntegral(value);
            case INTEGER:
                return value -> (int) DecimalUtils.castToIntegral(value);
            case BIGINT:
                return DecimalUtils::castToIntegral;
            case FLOAT:
                return value -> (float) DecimalUtils.doubleValue(value);
            case DOUBLE:
                return DecimalUtils::doubleValue;
            default:
                return null;
        }
    }
}
