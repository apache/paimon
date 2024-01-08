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

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Describe a table whose schema is merged from all source tables. */
public class AllMergedJdbcTableInfo implements JdbcTableInfo {

    private final List<Identifier> fromTables;
    private Schema schema;

    public AllMergedJdbcTableInfo() {
        this.fromTables = new ArrayList<>();
    }

    public void init(Identifier identifier, Schema schema) {
        this.fromTables.add(identifier);
        this.schema = schema;
    }

    public AllMergedJdbcTableInfo merge(Identifier otherTableId, Schema other) {
        schema = JdbcSchemaUtils.mergeSchema(location(), schema, otherTableId.getFullName(), other);
        fromTables.add(otherTableId);
        return this;
    }

    @Override
    public String location() {
        return String.format(
                "{%s}",
                fromTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));
    }

    @Override
    public List<Identifier> identifiers() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support converting to identifiers.");
    }

    @Override
    public String tableName() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support getting table name.");
    }

    @Override
    public String toPaimonTableName() {
        throw new UnsupportedOperationException(
                "AllMergedRichMySqlSchema doesn't support converting to Paimon table name.");
    }

    @Override
    public Schema schema() {
        return checkNotNull(schema, "MySqlSchema hasn't been set.");
    }
}
