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
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.DateTimeUtils;

/**
 * {@link DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE} to {@link
 * DataTypeRoot#TIMESTAMP_WITHOUT_TIME_ZONE} cast rule. Check and adjust if there is the precision
 * changes.
 */
class TimestampToTimestampCastRule extends AbstractCastRule<Timestamp, Timestamp> {

    static final TimestampToTimestampCastRule INSTANCE = new TimestampToTimestampCastRule();

    private TimestampToTimestampCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .target(DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                        .build());
    }

    @Override
    public CastExecutor<Timestamp, Timestamp> create(DataType inputType, DataType targetType) {
        return value -> DateTimeUtils.truncate(value, ((TimestampType) targetType).getPrecision());
    }
}
