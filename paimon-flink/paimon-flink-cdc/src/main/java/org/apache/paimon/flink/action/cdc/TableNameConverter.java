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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.catalog.Identifier;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Used to convert a MySQL source table name to corresponding Paimon table name. */
public class TableNameConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean caseSensitive;
    private final boolean mergeShards;
    private final String prefix;
    private final String suffix;
    private final Map<String, String> tableMapping;

    public TableNameConverter(boolean caseSensitive) {
        this(caseSensitive, true, "", "", null);
    }

    public TableNameConverter(
            boolean caseSensitive,
            boolean mergeShards,
            String prefix,
            String suffix,
            Map<String, String> tableMapping) {
        this.caseSensitive = caseSensitive;
        this.mergeShards = mergeShards;
        this.prefix = prefix;
        this.suffix = suffix;
        this.tableMapping = lowerMapKey(tableMapping);
    }

    public String convert(String originName) {
        if (tableMapping.containsKey(originName.toLowerCase())) {
            String mappedName = tableMapping.get(originName.toLowerCase());
            return caseSensitive ? mappedName : mappedName.toLowerCase();
        }

        String tableName = caseSensitive ? originName : originName.toLowerCase();
        return prefix + tableName + suffix;
    }

    public String convert(Identifier originIdentifier) {
        String rawName =
                mergeShards
                        ? originIdentifier.getObjectName()
                        : originIdentifier.getDatabaseName()
                                + "_"
                                + originIdentifier.getObjectName();
        return convert(rawName);
    }

    private Map<String, String> lowerMapKey(Map<String, String> map) {
        int size = map == null ? 0 : map.size();
        Map<String, String> lowerKeyMap = new HashMap<>(size);
        if (size == 0) {
            return lowerKeyMap;
        }

        for (String key : map.keySet()) {
            lowerKeyMap.put(key.toLowerCase(), map.get(key));
        }

        return lowerKeyMap;
    }
}
