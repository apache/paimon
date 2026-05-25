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

package org.apache.paimon.cli.sql;

import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

/** Tagged result from {@link SqlParser}: either a Calcite SqlNode or a meta-command. */
public class ParseResult {

    /** The type of parsed statement. */
    public enum Type {
        QUERY,
        SHOW_DATABASES,
        SHOW_TABLES,
        USE_DATABASE
    }

    private final Type type;
    @Nullable private final SqlNode sqlNode;
    @Nullable private final String database;
    @Nullable private final String originalSql;

    private ParseResult(
            Type type,
            @Nullable SqlNode sqlNode,
            @Nullable String database,
            @Nullable String originalSql) {
        this.type = type;
        this.sqlNode = sqlNode;
        this.database = database;
        this.originalSql = originalSql;
    }

    public static ParseResult query(SqlNode node, String originalSql) {
        return new ParseResult(Type.QUERY, node, null, originalSql);
    }

    public static ParseResult showDatabases() {
        return new ParseResult(Type.SHOW_DATABASES, null, null, null);
    }

    public static ParseResult showTables(@Nullable String database) {
        return new ParseResult(Type.SHOW_TABLES, null, database, null);
    }

    public static ParseResult useDatabase(String database) {
        return new ParseResult(Type.USE_DATABASE, null, database, null);
    }

    public Type type() {
        return type;
    }

    @Nullable
    public SqlNode sqlNode() {
        return sqlNode;
    }

    @Nullable
    public String database() {
        return database;
    }

    @Nullable
    public String originalSql() {
        return originalSql;
    }
}
