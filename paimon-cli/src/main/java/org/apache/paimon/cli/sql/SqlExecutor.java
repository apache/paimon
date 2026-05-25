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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.cli.CommandContext;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Executes SQL against Paimon tables via Apache Calcite's query planner and JDBC interface. */
public class SqlExecutor {

    private static final int DEFAULT_LIMIT = 100;
    private static final Pattern SCHEMA_REF =
            Pattern.compile("(?i)(?:FROM|JOIN|INTO)\\s+([a-zA-Z_][a-zA-Z0-9_]*)\\.");

    @Nullable private String defaultDatabase;
    @Nullable private Connection connection;
    @Nullable private SchemaPlus rootSchema;
    @Nullable private Catalog catalog;
    private final Set<String> registeredSchemas = new HashSet<>();

    public void setDefaultDatabase(String database) {
        this.defaultDatabase = database;
    }

    @Nullable
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public void execute(CommandContext ctx, ParseResult result) throws Exception {
        switch (result.type()) {
            case SHOW_DATABASES:
                executeShowDatabases(ctx);
                break;
            case SHOW_TABLES:
                executeShowTables(ctx, result.database());
                break;
            case USE_DATABASE:
                executeUseDatabase(result.database());
                break;
            case QUERY:
                executeQuery(ctx, result);
                break;
        }
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // best effort
            }
            connection = null;
        }
    }

    private void executeQuery(CommandContext ctx, ParseResult result) throws Exception {
        Connection conn = getOrCreateConnection(ctx);

        String sql = result.originalSql();

        // Register schemas referenced in the SQL on demand
        ensureSchemasForSql(sql);
        if (defaultDatabase != null) {
            ensureSchema(defaultDatabase);
            conn.setSchema(defaultDatabase);
        }

        if (!hasLimit(result) && !isModifyingStatement(sql)) {
            sql = sql + " LIMIT " + DEFAULT_LIMIT;
        }

        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            printResultSet(rs);
        }
    }

    private void ensureSchemasForSql(String sql) {
        Matcher m = SCHEMA_REF.matcher(sql);
        while (m.find()) {
            ensureSchema(m.group(1));
        }
    }

    private void ensureSchema(String dbName) {
        if (registeredSchemas.contains(dbName)) {
            return;
        }
        rootSchema.add(dbName, new PaimonSchema(catalog, dbName));
        registeredSchemas.add(dbName);
    }

    private Connection getOrCreateConnection(CommandContext ctx) throws Exception {
        if (connection != null) {
            return connection;
        }

        catalog = ctx.getCatalog();

        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        info.setProperty("caseSensitive", "false");
        info.setProperty("unquotedCasing", "UNCHANGED");
        info.setProperty("quotedCasing", "UNCHANGED");

        Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
        rootSchema = calciteConn.getRootSchema();

        connection = conn;
        return conn;
    }

    private static boolean hasLimit(ParseResult result) {
        if (result.sqlNode() == null) {
            return false;
        }
        String sqlUpper = result.originalSql().toUpperCase();
        return sqlUpper.contains(" LIMIT ");
    }

    private static boolean isModifyingStatement(String sql) {
        String upper = sql.trim().toUpperCase();
        return upper.startsWith("INSERT")
                || upper.startsWith("UPDATE")
                || upper.startsWith("DELETE");
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();

        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
            if (i > 1) {
                header.append("\t");
            }
            header.append(meta.getColumnLabel(i));
        }
        System.out.println(header.toString());

        int count = 0;
        while (rs.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= cols; i++) {
                if (i > 1) {
                    row.append("\t");
                }
                Object val = rs.getObject(i);
                row.append(val == null ? "NULL" : val);
            }
            System.out.println(row.toString());
            count++;
        }
        System.err.println("(" + count + " rows)");
    }

    private void executeShowDatabases(CommandContext ctx) throws Exception {
        for (String db : ctx.getCatalog().listDatabases()) {
            System.out.println(db);
        }
    }

    private void executeShowTables(CommandContext ctx, @Nullable String database) throws Exception {
        String db = database != null ? database : defaultDatabase;
        if (db == null) {
            System.err.println(
                    "No database selected. Use 'USE <database>' or 'SHOW TABLES IN <database>'.");
            return;
        }
        for (String t : ctx.getCatalog().listTables(db)) {
            System.out.println(t);
        }
    }

    private void executeUseDatabase(String database) {
        this.defaultDatabase = database;
        System.out.println("Using database '" + database + "'.");
    }
}
