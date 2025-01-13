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
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.utils.DecimalUtils;

/** {@link DataTypeRoot#BOOLEAN} to {@link DataTypeFamily#NUMERIC} conversions. */
class BooleanToNumericCastRule extends AbstractCastRule<Boolean, Object> {

    static final BooleanToNumericCastRule INSTANCE = new BooleanToNumericCastRule();

    private BooleanToNumericCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.BOOLEAN)
                        .target(DataTypeFamily.NUMERIC)
                        .build());
    }

    @Override
    public CastExecutor<Boolean, Object> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return value -> (byte) toInt(value);
            case SMALLINT:
                return value -> (short) toInt(value);
            case INTEGER:
                return this::toInt;
            case BIGINT:
                return value -> (long) toInt(value);
            case FLOAT:
                return value -> (float) toInt(value);
            case DOUBLE:
                return value -> (double) toInt(value);
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) targetType;
                return value ->
                        DecimalUtils.castFrom(
                                toInt(value), decimalType.getPrecision(), decimalType.getScale());
            default:
                return null;
        }
    }

    private int toInt(boolean bool) {
        return bool ? 1 : 0;
    }
}
