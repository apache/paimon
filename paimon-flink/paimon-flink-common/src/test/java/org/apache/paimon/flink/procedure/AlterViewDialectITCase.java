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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.RESTCatalogITCaseBase;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link AlterViewDialectProcedure}. */
public class AlterViewDialectITCase extends RESTCatalogITCaseBase {

    @Test
    public void testAlterViewDialect() {

        sql(
                String.format(
                        "INSERT INTO %s.%s VALUES ('1', 11), ('2', 22)",
                        DATABASE_NAME, TABLE_NAME));
        String viewName = "view_test";
        String query =
                String.format("SELECT * FROM `%s`.`%s` WHERE `b` > 1", DATABASE_NAME, TABLE_NAME);
        sql(String.format("CREATE VIEW %s.%s AS %s", DATABASE_NAME, viewName, query));
        String newQuery =
                String.format("SELECT * FROM `%s`.`%s` WHERE `b` > 2", DATABASE_NAME, TABLE_NAME);
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'update', `query` => '%s')",
                        DATABASE_NAME, viewName, newQuery));
        List<Row> result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains(newQuery);
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'drop')",
                        DATABASE_NAME, viewName));
        result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains("`b` > 1");
        sql(
                String.format(
                        "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'add', `query` => '%s')",
                        DATABASE_NAME, viewName, newQuery));
        result = sql(String.format("SHOW CREATE VIEW %s.%s", DATABASE_NAME, viewName));
        assertThat(result.toString()).contains(newQuery);

        result =
                sql(
                        String.format(
                                "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'add', `query` => '%s', `engine` => 'spark')",
                                DATABASE_NAME, viewName, newQuery));
        assertThat(result.toString()).contains("Success");
        result =
                sql(
                        String.format(
                                "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'update', `query` => '%s', `engine` => 'spark')",
                                DATABASE_NAME, viewName, query));
        assertThat(result.toString()).contains("Success");
        result =
                sql(
                        String.format(
                                "CALL sys.alter_view_dialect(`view` => '%s.%s', `action` => 'drop', `engine` => 'spark')",
                                DATABASE_NAME, viewName, query));
        assertThat(result.toString()).contains("Success");
    }
}
