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

import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

/** Used to convert a MySQL source table name to corresponding Paimon table name. */
public class TableNameConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean caseSensitive;
    private final boolean mergeShards;
    private final Map<String, String> dbPrefix;
    private final Map<String, String> dbSuffix;
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
        this(
                caseSensitive,
                mergeShards,
                new HashMap<>(),
                new HashMap<>(),
                prefix,
                suffix,
                tableMapping);
    }

    public TableNameConverter(
            boolean caseSensitive,
            boolean mergeShards,
            Map<String, String> dbPrefix,
            Map<String, String> dbSuffix,
            String prefix,
            String suffix,
            Map<String, String> tableMapping) {
        this.caseSensitive = caseSensitive;
        this.mergeShards = mergeShards;
        this.dbPrefix = dbPrefix;
        this.dbSuffix = dbSuffix;
        this.prefix = prefix;
        this.suffix = suffix;
        this.tableMapping = lowerMapKey(tableMapping);
    }

    public String convert(String originDbName, String originTblName) {
        // top priority: table mapping
        if (tableMapping.containsKey(originTblName.toLowerCase())) {
            String mappedName = tableMapping.get(originTblName.toLowerCase());
            return toLowerCaseIfNeed(mappedName, caseSensitive);
        }

        String tblPrefix = prefix;
        String tblSuffix = suffix;

        // second priority: prefix and postfix specified by db
        if (dbPrefix.containsKey(originDbName.toLowerCase())) {
            tblPrefix = dbPrefix.get(originDbName.toLowerCase());
        }
        if (dbSuffix.containsKey(originDbName.toLowerCase())) {
            tblSuffix = dbSuffix.get(originDbName.toLowerCase());
        }

        // third priority: normal prefix and suffix
        String tableName = toLowerCaseIfNeed(originDbName, caseSensitive);
        return tblPrefix + tableName + tblSuffix;
    }

    public String convert(Identifier originIdentifier) {
        String rawName =
                mergeShards
                        ? originIdentifier.getObjectName()
                        : originIdentifier.getDatabaseName()
                                + "_"
                                + originIdentifier.getObjectName();
        return convert(originIdentifier.getDatabaseName(), rawName);
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
