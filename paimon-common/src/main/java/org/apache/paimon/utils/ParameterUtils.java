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

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeJsonParser;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is a util class for converting string parameter to another format. */
public class ParameterUtils {

    public static List<Map<String, String>> getPartitions(String... partitionStrings) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : partitionStrings) {
            partitions.add(parseCommaSeparatedKeyValues(partition));
        }
        return partitions;
    }

    @Nullable
    public static Predicate toPartitionPredicate(
            List<Map<String, String>> partitionList,
            RowType partitionType,
            String partitionDefaultName) {
        if (partitionList == null || partitionList.isEmpty()) {
            return null;
        }
        return PredicateBuilder.or(
                partitionList.stream()
                        .map(
                                p ->
                                        PredicateBuilder.partition(
                                                p, partitionType, partitionDefaultName))
                        .toArray(Predicate[]::new));
    }

    public static Map<String, String> parseCommaSeparatedKeyValues(String keyValues) {
        Map<String, String> kvs = new HashMap<>();
        if (!StringUtils.isNullOrWhitespaceOnly(keyValues)) {
            for (String kvString : keyValues.split(",")) {
                parseKeyValueString(kvs, kvString);
            }
        }
        return kvs;
    }

    public static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }

    public static void parseKeyValueList(Map<String, List<String>> mapList, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        String[] valueArr = kv[1].trim().split(",");
        List<String> valueList = new ArrayList<>();
        for (String value : valueArr) {
            valueList.add(value);
        }
        mapList.put(kv[0].trim(), valueList);
    }

    public static List<DataField> parseDataFieldArray(String data) {
        List<DataField> list = new ArrayList<>();
        if (data != null) {
            JsonNode jsonArray = JsonSerdeUtil.fromJson(data, JsonNode.class);
            if (jsonArray.isArray()) {
                for (JsonNode objNode : jsonArray) {
                    DataField dataField = DataTypeJsonParser.parseDataField(objNode);
                    list.add(dataField);
                }
            }
        }
        return list;
    }
}
