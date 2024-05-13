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

package org.apache.paimon.tests;

import static org.assertj.core.api.Assertions.assertThat;

/** Base e2e test class for hive and spark readers. */
public abstract class E2eReaderTestBase extends E2eTestBase {
    public E2eReaderTestBase(boolean withKafka, boolean withHive, boolean withSpark) {
        super(withKafka, withHive, withSpark);
    }

    protected String createCatalogSql(String catalog, String warehouse, String... options) {
        return String.join(
                "\n",
                String.format("CREATE CATALOG %s WITH (", catalog),
                "  'type' = 'paimon',",
                options.length > 0 ? String.join(",", options) + "," : "",
                String.format("  'warehouse' = '%s'", warehouse),
                ");",
                "",
                String.format("USE CATALOG %s;", catalog));
    }

    protected String createTableSql(String tableName) {
        return String.format(
                "CREATE TABLE %s (" + "  a int," + "  b bigint," + "  c string" + ");", tableName);
    }

    protected String createInsertSql(String tableName) {
        return String.format(
                "INSERT INTO %s VALUES "
                        + "(1, 10, 'Hi'), "
                        + "(1, 100, 'Hi Again'), "
                        + "(2, 20, 'Hello'), "
                        + "(3, 30, 'Table'), "
                        + "(4, 40, 'Store');",
                tableName);
    }

    protected void checkQueryResults(String tableName, E2eQueryExecutor executor) throws Exception {
        checkQueryResults(tableName, executor, "");
    }

    protected void checkQueryResults(String tableName, E2eQueryExecutor executor, String ddl)
            throws Exception {
        checkQueryResult(
                executor,
                ddl + String.format("SELECT * FROM %s ORDER BY b;", tableName),
                "1\t10\tHi\n"
                        + "2\t20\tHello\n"
                        + "3\t30\tTable\n"
                        + "4\t40\tStore\n"
                        + "1\t100\tHi Again\n");
        checkQueryResult(
                executor,
                ddl + String.format("SELECT b, a FROM %s ORDER BY b;", tableName),
                "10\t1\n" + "20\t2\n" + "30\t3\n" + "40\t4\n" + "100\t1\n");
        checkQueryResult(
                executor,
                ddl + String.format("SELECT * FROM %s WHERE a > 1 ORDER BY b;", tableName),
                "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                executor,
                ddl
                        + String.format(
                                "SELECT a, SUM(b), MIN(c) FROM %s GROUP BY a ORDER BY a;",
                                tableName),
                "1\t110\tHi\n" + "2\t20\tHello\n" + "3\t30\tTable\n" + "4\t40\tStore\n");
        checkQueryResult(
                executor,
                ddl
                        + String.format(
                                "SELECT T1.a, T1.b, T2.b FROM %s T1 JOIN %s T2 "
                                        + "ON T1.a = T2.a WHERE T1.a <= 2 ORDER BY T1.a, T1.b, T2.b;",
                                tableName, tableName),
                "1\t10\t10\n" + "1\t10\t100\n" + "1\t100\t10\n" + "1\t100\t100\n" + "2\t20\t20\n");
    }

    protected void checkQueryResult(E2eQueryExecutor executor, String query, String result)
            throws Exception {
        final String file = "pk.hql";
        writeSharedFile(file, query);
        assertThat(executor.execute(file)).isEqualTo(result);
    }
}
