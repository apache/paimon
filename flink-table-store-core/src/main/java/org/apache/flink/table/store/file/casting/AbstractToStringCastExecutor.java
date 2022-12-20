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

package org.apache.flink.table.store.file.casting;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;

import javax.annotation.Nullable;

/**
 * Cast input to string base implementation.
 *
 * @param <IN> the input value type.
 */
public abstract class AbstractToStringCastExecutor<IN> implements CastExecutor<IN, StringData> {
    protected final boolean targetCharType;
    protected final int targetLength;

    AbstractToStringCastExecutor(boolean targetCharType, int targetLength) {
        this.targetCharType = targetCharType;
        this.targetLength = targetLength;
    }

    @Nullable
    @Override
    public StringData cast(@Nullable IN value) throws TableException {
        boolean inputIsNull = value == null;
        BinaryStringData result;
        if (!inputIsNull) {
            String strVal = getString(value);
            BinaryStringData strData = BinaryStringData.fromString(strVal);
            if (strData.numChars() > targetLength) {
                result = BinaryStringData.fromString(strVal.substring(0, targetLength));
            } else {
                if (strData.numChars() < targetLength) {
                    if (targetCharType) {
                        int padLength = targetLength - strData.numChars();
                        BinaryStringData padString = BinaryStringData.blankString(padLength);
                        result = BinaryStringDataUtil.concat(strData, padString);
                    } else {
                        result = strData;
                    }
                } else {
                    result = strData;
                }
            }
        } else {
            result = BinaryStringData.EMPTY_UTF8;
        }

        return result;
    }

    abstract String getString(IN value);
}
