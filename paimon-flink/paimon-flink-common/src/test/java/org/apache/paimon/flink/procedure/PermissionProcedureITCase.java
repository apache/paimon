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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for REST permission procedures. */
public class PermissionProcedureITCase extends RESTCatalogITCaseBase {

    @BeforeEach
    @Override
    public void before() throws IOException {
        super.before();
        restCatalogServer.registerManagementPrincipal("analyst");
        restCatalogServer.registerManagementPrincipal("first");
        restCatalogServer.registerManagementPrincipal("second");
    }

    @Test
    public void testGrantListAndRevoke() {
        assertThat(grantTable("select", "2027-01-01T00:00:00Z")).containsExactly(Row.of(true));
        assertThat(grantTable("SELECT", "2028-01-01T00:00:00Z")).containsExactly(Row.of(true));

        List<Row> listed =
                sql(
                        "CALL sys.list_permissions("
                                + "resource_type => 'TABLE', "
                                + "`database` => '%s', "
                                + "`table` => '%s', "
                                + "principal => 'analyst')",
                        DATABASE_NAME, TABLE_NAME);
        assertThat(listed).hasSize(1);
        Row assignment = listed.get(0);
        assertThat(assignment.getField(0)).isEqualTo("TABLE");
        assertThat(assignment.getField(1)).isEqualTo(DATABASE_NAME);
        assertThat(assignment.getField(2)).isEqualTo(TABLE_NAME);
        assertThat(assignment.getField(3)).isNull();
        assertThat(assignment.getField(4)).isNull();
        assertThat(assignment.getField(5)).isEqualTo("SELECT");
        assertThat(assignment.getField(6)).isEqualTo("analyst");
        assertThat(assignment.getField(7)).isNull();
        assertThat(assignment.getField(8)).isNull();
        assertThat(assignment.getField(9)).isEqualTo("2028-01-01T00:00:00Z");
        assertThat(assignment.getField(10)).isNull();

        String revoke =
                String.format(
                        "CALL sys.revoke_permission("
                                + "resource_type => 'TABLE', "
                                + "access => 'SELECT', "
                                + "principal => 'analyst', "
                                + "`database` => '%s', "
                                + "`table` => '%s')",
                        DATABASE_NAME, TABLE_NAME);
        assertThat(sql(revoke)).containsExactly(Row.of(true));
        assertThat(sql(revoke)).containsExactly(Row.of(true));
        assertThat(
                        sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'TABLE', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME, TABLE_NAME))
                .isEmpty();
    }

    @Test
    public void testListPermissionsPagination() {
        grantCatalog("first");
        grantCatalog("second");

        Row first =
                sql("CALL sys.list_permissions(resource_type => 'CATALOG', max_results => 1)")
                        .get(0);
        assertThat(first.getField(6)).isEqualTo("first");
        String pageToken = (String) first.getField(10);
        assertThat(pageToken).isNotEmpty().isNotEqualTo("1");

        sql(
                "CALL sys.revoke_permission("
                        + "resource_type => 'CATALOG', "
                        + "access => 'CREATEDATABASE', "
                        + "principal => 'first')");

        Row second =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'CATALOG', "
                                        + "max_results => 1, "
                                        + "page_token => '%s')",
                                pageToken)
                        .get(0);
        assertThat(second.getField(6)).isEqualTo("second");
        assertThat(second.getField(10)).isNull();
    }

    @Test
    public void testDescendantScopes() {
        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'CATALOG_ALL', "
                                        + "access => 'SELECT', "
                                        + "principal => 'analyst')"))
                .containsExactly(Row.of(true));
        Row catalogAll =
                sql("CALL sys.list_permissions("
                                + "resource_type => 'CATALOG_ALL', "
                                + "principal => 'analyst')")
                        .get(0);
        assertThat(catalogAll.getField(0)).isEqualTo("CATALOG_ALL");
        assertThat(catalogAll.getField(1)).isNull();
        assertThat(catalogAll.getField(5)).isEqualTo("SELECT");

        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'DATABASE_ALL', "
                                        + "`database` => '%s', "
                                        + "access => 'UPDATE', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME))
                .containsExactly(Row.of(true));
        Row databaseAll =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'DATABASE_ALL', "
                                        + "`database` => '%s', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME)
                        .get(0);
        assertThat(databaseAll.getField(0)).isEqualTo("DATABASE_ALL");
        assertThat(databaseAll.getField(1)).isEqualTo(DATABASE_NAME);
        assertThat(databaseAll.getField(2)).isNull();
        assertThat(databaseAll.getField(5)).isEqualTo("UPDATE");
    }

    @Test
    public void testColumnPermissions() {
        sql("ALTER TABLE %s.%s SET ('query-auth.enabled' = 'true')", DATABASE_NAME, TABLE_NAME);
        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'COLUMN', "
                                        + "access => 'SELECT', "
                                        + "principal => 'analyst', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "column_names => '[\"a\"]')",
                                DATABASE_NAME, TABLE_NAME))
                .containsExactly(Row.of(true));

        Row included =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME, TABLE_NAME)
                        .get(0);
        assertThat(stringArray(included.getField(7))).containsExactly("a");
        assertThat(included.getField(8)).isNull();

        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'COLUMN', "
                                        + "access => 'SELECT', "
                                        + "principal => 'analyst', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "excluded_column_names => '[\"b\"]')",
                                DATABASE_NAME, TABLE_NAME))
                .containsExactly(Row.of(true));
        Row excluded =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME, TABLE_NAME)
                        .get(0);
        assertThat(excluded.getField(7)).isNull();
        assertThat(stringArray(excluded.getField(8))).containsExactly("b");
    }

    @Test
    public void testPositionalGrant() {
        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "'TABLE', 'SELECT', 'analyst', '%s', '%s', '', '', "
                                        + "'2027-01-01T00:00:00Z')",
                                DATABASE_NAME, TABLE_NAME))
                .containsExactly(Row.of(true));
        Row assignment = listedTable().get(0);
        assertThat(assignment.getField(1)).isEqualTo(DATABASE_NAME);
        assertThat(assignment.getField(2)).isEqualTo(TABLE_NAME);
        assertThat(assignment.getField(5)).isEqualTo("SELECT");
        assertThat(assignment.getField(6)).isEqualTo("analyst");
        assertThat(assignment.getField(9)).isEqualTo("2027-01-01T00:00:00Z");

        sql("ALTER TABLE %s.%s SET ('query-auth.enabled' = 'true')", DATABASE_NAME, TABLE_NAME);
        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "'COLUMN', 'SELECT', 'analyst', '%s', '%s', '', '', '', "
                                        + "'[\"a\"]', '')",
                                DATABASE_NAME, TABLE_NAME))
                .containsExactly(Row.of(true));
        Row columns =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => '%s', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME, TABLE_NAME)
                        .get(0);
        assertThat(stringArray(columns.getField(7))).containsExactly("a");
        assertThat(columns.getField(8)).isNull();
    }

    @Test
    public void testColumnNameIdentity() {
        sql(
                "CREATE TABLE %s.space_columns (`secret` STRING, ` secret ` STRING) "
                        + "WITH ('query-auth.enabled' = 'true')",
                DATABASE_NAME);
        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'COLUMN', "
                                        + "access => 'SELECT', "
                                        + "principal => 'analyst', "
                                        + "`database` => '%s', "
                                        + "`table` => 'space_columns', "
                                        + "excluded_column_names => '[\" secret \"]')",
                                DATABASE_NAME))
                .containsExactly(Row.of(true));
        Row excluded =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => 'space_columns', "
                                        + "principal => 'analyst')",
                                DATABASE_NAME)
                        .get(0);
        assertThat(excluded.getField(7)).isNull();
        assertThat(stringArray(excluded.getField(8))).containsExactly(" secret ");

        assertThat(
                        sql(
                                "CALL sys.grant_permission("
                                        + "resource_type => 'COLUMN', "
                                        + "access => 'SELECT', "
                                        + "principal => 'first', "
                                        + "`database` => '%s', "
                                        + "`table` => 'space_columns', "
                                        + "column_names => '[\" secret \", \"secret\"]')",
                                DATABASE_NAME))
                .containsExactly(Row.of(true));
        Row included =
                sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => 'space_columns', "
                                        + "principal => 'first')",
                                DATABASE_NAME)
                        .get(0);
        assertThat(stringArray(included.getField(7))).containsExactly(" secret ", "secret");
        assertThat(included.getField(8)).isNull();

        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'COLUMN', "
                                                + "access => 'SELECT', "
                                                + "principal => 'second', "
                                                + "`database` => '%s', "
                                                + "`table` => 'space_columns', "
                                                + "excluded_column_names => ' secret ')",
                                        DATABASE_NAME))
                .hasMessageContaining("JSON array of strings");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'COLUMN', "
                                                + "access => 'SELECT', "
                                                + "principal => 'second', "
                                                + "`database` => '%s', "
                                                + "`table` => 'space_columns', "
                                                + "column_names => '[\"a\"],[\"b\"]')",
                                        DATABASE_NAME))
                .hasMessageContaining("JSON array of strings");
        assertThat(
                        sql(
                                "CALL sys.list_permissions("
                                        + "resource_type => 'COLUMN', "
                                        + "`database` => '%s', "
                                        + "`table` => 'space_columns', "
                                        + "principal => 'second')",
                                DATABASE_NAME))
                .isEmpty();
    }

    @Test
    public void testRejectsInvalidArguments() {
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'NOPE', "
                                                + "access => 'SELECT', "
                                                + "principal => 'analyst')"))
                .hasMessageContaining("Invalid resource_type 'NOPE'");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'CATALOG', "
                                                + "access => 'CREATEDATABASE', "
                                                + "principal => '')"))
                .hasMessageContaining("principal cannot be empty.");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'COLUMN', "
                                                + "access => 'SELECT', "
                                                + "principal => 'analyst', "
                                                + "`database` => '%s', "
                                                + "`table` => '%s', "
                                                + "column_names => '[\"a\"]', "
                                                + "excluded_column_names => '[\"b\"]')",
                                        DATABASE_NAME, TABLE_NAME))
                .hasMessageContaining(
                        "columns must contain exactly one of columnNames or excludedColumnNames.");
        assertThatThrownBy(
                        () ->
                                sql(
                                        "CALL sys.grant_permission("
                                                + "resource_type => 'TABLE', "
                                                + "access => 'SELECT', "
                                                + "principal => 'analyst', "
                                                + "`database` => '%s', "
                                                + "`table` => '%s', "
                                                + "column_names => '[\"a\"]')",
                                        DATABASE_NAME, TABLE_NAME))
                .hasMessageContaining("columns is only valid for COLUMN resource.");
    }

    private List<Row> listedTable() {
        return sql(
                "CALL sys.list_permissions("
                        + "resource_type => 'TABLE', "
                        + "`database` => '%s', "
                        + "`table` => '%s', "
                        + "principal => 'analyst')",
                DATABASE_NAME, TABLE_NAME);
    }

    private List<Row> grantTable(String access, String expireTime) {
        return sql(
                "CALL sys.grant_permission("
                        + "resource_type => 'table', "
                        + "access => '%s', "
                        + "principal => 'analyst', "
                        + "`database` => '%s', "
                        + "`table` => '%s', "
                        + "expire_time => '%s')",
                access, DATABASE_NAME, TABLE_NAME, expireTime);
    }

    private void grantCatalog(String principal) {
        sql(
                "CALL sys.grant_permission("
                        + "resource_type => 'CATALOG', "
                        + "access => 'CREATEDATABASE', "
                        + "principal => '%s')",
                principal);
    }

    private static String[] stringArray(Object value) {
        if (value instanceof String[]) {
            return (String[]) value;
        }
        Object[] objects = (Object[]) value;
        String[] strings = new String[objects.length];
        for (int i = 0; i < objects.length; i++) {
            strings[i] = String.valueOf(objects[i]);
        }
        return strings;
    }
}
