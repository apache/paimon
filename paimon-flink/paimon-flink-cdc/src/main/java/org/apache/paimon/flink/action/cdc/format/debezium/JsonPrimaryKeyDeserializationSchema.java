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

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * This class is used to deserialize byte[] messages into String format, and then add primary key
 * fields to the JSON string.
 */
public class JsonPrimaryKeyDeserializationSchema implements DeserializationSchema<String> {

    public static final String PRIMARY_KEY_NAMES = "pkNames";
    private final List<String> primaryKeyNames;

    public JsonPrimaryKeyDeserializationSchema(List<String> primaryKeyNames) {
        checkNotNull(primaryKeyNames);
        this.primaryKeyNames = primaryKeyNames;
    }

    @Override
    public String deserialize(byte[] message) {
        try {
            String value = new String(message, StandardCharsets.UTF_8);
            return JsonSerdeUtil.putArrayToJsonString(value, PRIMARY_KEY_NAMES, primaryKeyNames);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize message", e);
        }
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
