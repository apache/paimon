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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.utils.BinaryStringUtils;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} to {@link DataTypeFamily#INTEGER_NUMERIC} and {@link
 * DataTypeFamily#APPROXIMATE_NUMERIC} cast rule.
 */
class StringToNumericPrimitiveCastRule extends AbstractCastRule<BinaryString, Number> {

    static final StringToNumericPrimitiveCastRule INSTANCE = new StringToNumericPrimitiveCastRule();

    private StringToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, Number> create(DataType inputType, DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case TINYINT:
                return BinaryStringUtils::toByte;
            case SMALLINT:
                return BinaryStringUtils::toShort;
            case INTEGER:
                return BinaryStringUtils::toInt;
            case BIGINT:
                return BinaryStringUtils::toLong;
            case FLOAT:
                return BinaryStringUtils::toFloat;
            case DOUBLE:
                return BinaryStringUtils::toDouble;
            default:
                return null;
        }
    }
}
