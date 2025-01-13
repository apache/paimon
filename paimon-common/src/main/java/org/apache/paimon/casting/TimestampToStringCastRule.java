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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.TimeZone;

import static org.apache.paimon.types.VarCharType.STRING_TYPE;

/** {@link DataTypeFamily#TIMESTAMP} to {@link DataTypeFamily#CHARACTER_STRING} cast rule. */
class TimestampToStringCastRule extends AbstractCastRule<Timestamp, BinaryString> {

    static final TimestampToStringCastRule INSTANCE = new TimestampToStringCastRule();

    private TimestampToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.TIMESTAMP)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, BinaryString> create(DataType inputType, DataType targetType) {
        final int precision = DataTypeChecks.getPrecision(inputType);
        TimeZone timeZone =
                inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        ? TimeZone.getDefault()
                        : DateTimeUtils.UTC_ZONE;
        return value ->
                BinaryString.fromString(DateTimeUtils.formatTimestamp(value, timeZone, precision));
    }
}
