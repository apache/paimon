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

package org.apache.paimon.flink.action.cdc.schema;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Describe a table whose schema is merged from shards (tables with the same name but in different
 * database).
 */
public class ShardsMergedJdbcTableInfo implements JdbcTableInfo {

    private final List<String> fromDatabases;

    private String tableName;

    private Schema schema;

    public ShardsMergedJdbcTableInfo() {
        this.fromDatabases = new ArrayList<>();
    }

    public void init(Identifier identifier, Schema schema) {
        this.fromDatabases.add(identifier.getDatabaseName());
        this.tableName = identifier.getObjectName();
        this.schema = schema;
    }

    public ShardsMergedJdbcTableInfo merge(Identifier otherTableId, Schema other) {
        checkArgument(
                otherTableId.getObjectName().equals(tableName),
                "Table to be merged '%s' should equals to current table name '%s'.",
                otherTableId.getObjectName(),
                tableName);

        schema = JdbcSchemaUtils.mergeSchema(location(), schema, otherTableId.getFullName(), other);
        fromDatabases.add(otherTableId.getDatabaseName());
        return this;
    }

    @Override
    public String location() {
        return String.format("[%s].%s", String.join(",", fromDatabases), tableName);
    }

    @Override
    public List<Identifier> identifiers() {
        return fromDatabases.stream()
                .map(databaseName -> Identifier.create(databaseName, tableName))
                .collect(Collectors.toList());
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public String toPaimonTableName() {
        return tableName;
    }

    @Override
    public Schema schema() {
        return checkNotNull(schema, "MySqlSchema hasn't been set.");
    }
}
