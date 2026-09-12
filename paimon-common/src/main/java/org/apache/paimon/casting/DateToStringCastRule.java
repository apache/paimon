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
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.BinaryStringUtils;
import org.apache.paimon.utils.DateTimeUtils;

/** {@link DataTypeRoot#DATE} to {@link DataTypeFamily#CHARACTER_STRING} cast rule. */
class DateToStringCastRule extends AbstractCastRule<Integer, BinaryString> {

    static final DateToStringCastRule INSTANCE = new DateToStringCastRule();

    private DateToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.DATE)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<Integer, BinaryString> create(DataType inputType, DataType targetType) {
        boolean padOrTrim =
                targetType.is(DataTypeRoot.CHAR)
                        || DataTypeChecks.getLength(targetType) != VarCharType.MAX_LENGTH;
        return value -> {
            BinaryString result = BinaryString.fromString(DateTimeUtils.formatDate(value));
            return padOrTrim ? BinaryStringUtils.toCharacterString(result, targetType) : result;
        };
    }
}
