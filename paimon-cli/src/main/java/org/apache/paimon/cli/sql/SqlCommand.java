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

import org.apache.paimon.cli.Command;
import org.apache.paimon.cli.CommandContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/** SQL command supporting one-shot queries and interactive REPL mode. */
public class SqlCommand implements Command {

    @Override
    public String name() {
        return "sql";
    }

    @Override
    public String description() {
        return "Execute SQL queries on Paimon tables";
    }

    @Override
    public void execute(CommandContext ctx, String[] args) throws Exception {
        SqlParser parser = new SqlParser();
        SqlExecutor executor = new SqlExecutor();

        if (args.length > 0) {
            String sql = String.join(" ", args);
            executeSql(ctx, parser, executor, sql);
        } else {
            runRepl(ctx, parser, executor);
        }
    }

    private void executeSql(
            CommandContext ctx, SqlParser parser, SqlExecutor executor, String sql) {
        try {
            ParseResult result = parser.parse(sql);
            executor.execute(ctx, result);
        } catch (IllegalArgumentException e) {
            System.err.println("SQL error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

    private void runRepl(CommandContext ctx, SqlParser parser, SqlExecutor executor)
            throws Exception {
        System.out.println("Paimon SQL (type 'help' for usage, 'exit' to quit)");
        System.out.println();

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sqlBuf = new StringBuilder();

        while (true) {
            String prompt = sqlBuf.length() == 0 ? "paimon> " : "     > ";
            System.out.print(prompt);
            System.out.flush();

            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String trimmed = line.trim();
            if (sqlBuf.length() == 0) {
                if ("exit".equalsIgnoreCase(trimmed) || "quit".equalsIgnoreCase(trimmed)) {
                    System.out.println("Bye!");
                    break;
                }
                if ("help".equalsIgnoreCase(trimmed)) {
                    printReplHelp();
                    continue;
                }
            }

            sqlBuf.append(line).append(" ");

            if (trimmed.endsWith(";")) {
                String sql = sqlBuf.toString().trim();
                if (sql.endsWith(";")) {
                    sql = sql.substring(0, sql.length() - 1).trim();
                }
                if (!sql.isEmpty()) {
                    executeSql(ctx, parser, executor, sql);
                }
                sqlBuf.setLength(0);
                System.out.println();
            }
        }
    }

    private void printReplHelp() {
        System.out.println("Supported SQL:");
        System.out.println(
                "  SELECT [DISTINCT] [cols|agg(col)|*] FROM [db.]table"
                        + " [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT n]");
        System.out.println("  SHOW DATABASES;");
        System.out.println("  SHOW TABLES [IN database];");
        System.out.println("  USE database;");
        System.out.println();
        System.out.println("Aggregate functions: COUNT, SUM, AVG, MIN, MAX");
        System.out.println();
        System.out.println("Commands:");
        System.out.println("  help   - Show this help");
        System.out.println("  exit   - Exit the REPL");
        System.out.println();
        System.out.println("SQL statements must end with ';'");
    }

    @Override
    public String usage() {
        return "Usage: paimon sql [QUERY]\n\n"
                + "Execute SQL queries on Paimon tables.\n\n"
                + "If QUERY is provided, execute it and exit.\n"
                + "If no QUERY is provided, start an interactive REPL.\n\n"
                + "Supported SQL:\n"
                + "  SELECT [DISTINCT] [cols|agg(col)|*] FROM [db.]table\n"
                + "    [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT n]\n"
                + "  SHOW DATABASES\n"
                + "  SHOW TABLES [IN database]\n"
                + "  USE database\n\n"
                + "Aggregate functions: COUNT(*), COUNT(col), SUM, AVG, MIN, MAX\n\n"
                + "Examples:\n"
                + "  paimon sql \"SELECT * FROM mydb.users WHERE age > 18 LIMIT 10\"\n"
                + "  paimon sql \"SELECT city, COUNT(*) FROM mydb.users GROUP BY city\"\n"
                + "  paimon sql \"SELECT DISTINCT city FROM mydb.users\"";
    }
}
