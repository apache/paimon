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

package org.apache.paimon.utils;

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

/** Utils for default value. */
public class DefaultValueUtils {

    public static Object convertDefaultValue(DataType dataType, String defaultValueStr) {
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolve =
                (CastExecutor<Object, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, dataType);

        if (resolve == null) {
            throw new RuntimeException("Default value do not support the type of " + dataType);
        }

        if (defaultValueStr.startsWith("'") && defaultValueStr.endsWith("'")) {
            defaultValueStr = defaultValueStr.substring(1, defaultValueStr.length() - 1);
        }

        return resolve.cast(BinaryString.fromString(defaultValueStr));
    }

    public static void validateDefaultValue(DataType dataType, @Nullable String defaultValueStr) {
        if (defaultValueStr == null) {
            return;
        }

        try {
            convertDefaultValue(dataType, defaultValueStr);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unsupported default value `" + defaultValueStr + "` for type " + dataType, e);
        }
    }
}
