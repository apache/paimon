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
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;

/**
 * {@link DataTypeRoot#TIME_WITHOUT_TIME_ZONE} to {@link DataTypeFamily#CHARACTER_STRING} cast rule.
 */
class TimeToStringCastRule extends AbstractCastRule<Integer, BinaryString> {

    static final TimeToStringCastRule INSTANCE = new TimeToStringCastRule();

    private TimeToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIME_WITHOUT_TIME_ZONE)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public CastExecutor<Integer, BinaryString> create(DataType inputType, DataType targetType) {
        return value ->
                BinaryString.fromString(
                        DateTimeUtils.formatTimestampMillis(
                                value, DataTypeChecks.getPrecision(inputType)));
    }
}
