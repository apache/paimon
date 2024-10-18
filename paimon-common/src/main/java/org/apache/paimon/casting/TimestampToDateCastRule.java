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

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.TimeZone;

/**
 * {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE}/{@link
 * DataTypeRoot#TIMESTAMP_WITH_LOCAL_TIME_ZONE} to {@link DataTypeRoot#DATE}.
 */
class TimestampToDateCastRule extends AbstractCastRule<Timestamp, Number> {

    static final TimestampToDateCastRule INSTANCE = new TimestampToDateCastRule();

    private TimestampToDateCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .input(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                        .target(DataTypeRoot.DATE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, Number> create(DataType inputType, DataType targetType) {
        if (inputType.is(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
            return value -> (int) (value.getMillisecond() / DateTimeUtils.MILLIS_PER_DAY);
        } else if (inputType.is(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
            return value ->
                    DateTimeUtils.timestampWithLocalZoneToDate(value, TimeZone.getDefault());
        }
        return null;
    }
}
