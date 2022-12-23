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

/** Cast string to string. */
public class StringToStringCastExecutor implements CastExecutor<StringData, StringData> {
    protected final boolean targetCharType;
    protected final int targetLength;

    public StringToStringCastExecutor(boolean targetCharType, int targetLength) {
        this.targetCharType = targetCharType;
        this.targetLength = targetLength;
    }

    @Override
    public StringData cast(StringData value) throws TableException {
        if (value == null) {
            return null;
        }
        StringData result;
        String strVal = value.toString();
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

        return result;
    }
}
