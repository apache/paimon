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

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.annotation.Nullable;

/** Thin SQL parser wrapper around Apache Calcite. */
public class SqlParser {

    private static final org.apache.calcite.sql.parser.SqlParser.Config PARSER_CONFIG =
            org.apache.calcite.sql.parser.SqlParser.config()
                    .withLex(Lex.MYSQL)
                    .withIdentifierMaxLength(256);

    public ParseResult parse(String sql) {
        String trimmed = sql.trim();
        if (trimmed.endsWith(";")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
        }

        String upper = trimmed.toUpperCase();
        if (upper.startsWith("SHOW DATABASES")) {
            return ParseResult.showDatabases();
        }
        if (upper.startsWith("SHOW TABLES")) {
            return ParseResult.showTables(extractDatabase(trimmed));
        }
        if (upper.startsWith("USE ")) {
            return ParseResult.useDatabase(trimmed.substring(4).trim());
        }

        try {
            org.apache.calcite.sql.parser.SqlParser calciteParser =
                    org.apache.calcite.sql.parser.SqlParser.create(trimmed, PARSER_CONFIG);
            SqlNode node = calciteParser.parseStmt();
            return ParseResult.query(node, trimmed);
        } catch (SqlParseException e) {
            throw new IllegalArgumentException("SQL parse error: " + e.getMessage(), e);
        }
    }

    @Nullable
    private static String extractDatabase(String sql) {
        String upper = sql.toUpperCase();
        int inIdx = upper.indexOf(" IN ");
        if (inIdx > 0) {
            return sql.substring(inIdx + 4).trim();
        }
        return null;
    }

    /** Strips backtick/double-quote quoting from Calcite's SqlNode.toString() output. */
    static String unquoteIdentifiers(String expr) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (c == '\'') {
                sb.append(c);
                i++;
                while (i < expr.length()) {
                    char ch = expr.charAt(i);
                    sb.append(ch);
                    if (ch == '\'') {
                        i++;
                        if (i < expr.length() && expr.charAt(i) == '\'') {
                            sb.append('\'');
                            i++;
                        } else {
                            break;
                        }
                    } else {
                        i++;
                    }
                }
            } else if (c == '`') {
                i++;
                while (i < expr.length() && expr.charAt(i) != '`') {
                    sb.append(expr.charAt(i));
                    i++;
                }
                if (i < expr.length()) {
                    i++;
                }
            } else {
                sb.append(c);
                i++;
            }
        }
        return sb.toString();
    }
}
