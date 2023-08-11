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

package org.apache.paimon.flink.action.cdc.mysql.schema;

import org.apache.paimon.catalog.Identifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class to manage MySQL tables and their schemas. */
public class MySqlSchemasInfo {

    private final Map<Identifier, MySqlSchema> pkTableSchemas;
    private final Map<Identifier, MySqlSchema> nonPkTableSchemas;

    public MySqlSchemasInfo() {
        this.pkTableSchemas = new HashMap<>();
        this.nonPkTableSchemas = new HashMap<>();
    }

    public void addSchema(Identifier identifier, MySqlSchema mysqlSchema) {
        if (mysqlSchema.primaryKeys().isEmpty()) {
            nonPkTableSchemas.put(identifier, mysqlSchema);
        } else {
            pkTableSchemas.put(identifier, mysqlSchema);
        }
    }

    public List<Identifier> pkTables() {
        return new ArrayList<>(pkTableSchemas.keySet());
    }

    public List<Identifier> nonPkTables() {
        return new ArrayList<>(nonPkTableSchemas.keySet());
    }

    // only merge pk tables now
    public MySqlTableInfo mergeAll() {
        boolean initialized = false;
        AllMergedMySqlTableInfo merged = new AllMergedMySqlTableInfo();
        for (Map.Entry<Identifier, MySqlSchema> entry : pkTableSchemas.entrySet()) {
            Identifier id = entry.getKey();
            MySqlSchema schema = entry.getValue();
            if (!initialized) {
                merged.init(id, schema);
                initialized = true;
            } else {
                merged.merge(id, schema);
            }
        }
        return merged;
    }

    // only handle pk tables now
    public List<MySqlTableInfo> toMySqlTableInfos(boolean mergeShards) {
        if (mergeShards) {
            return mergeShards();
        } else {
            return pkTableSchemas.entrySet().stream()
                    .map(e -> new UnmergedMySqlTableInfo(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }
    }

    // only merge pk tables now
    /** Merge schemas for tables that have the same table name. */
    private List<MySqlTableInfo> mergeShards() {
        Map<String, ShardsMergedMySqlTableInfo> nameSchemaMap = new HashMap<>();
        for (Map.Entry<Identifier, MySqlSchema> entry : pkTableSchemas.entrySet()) {
            Identifier id = entry.getKey();
            String tableName = id.getObjectName();

            MySqlSchema toBeMerged = entry.getValue();
            ShardsMergedMySqlTableInfo current = nameSchemaMap.get(tableName);
            if (current == null) {
                current = new ShardsMergedMySqlTableInfo();
                current.init(id, toBeMerged);
                nameSchemaMap.put(tableName, current);
            } else {
                nameSchemaMap.put(tableName, current.merge(id, toBeMerged));
            }
        }

        return new ArrayList<>(nameSchemaMap.values());
    }
}
