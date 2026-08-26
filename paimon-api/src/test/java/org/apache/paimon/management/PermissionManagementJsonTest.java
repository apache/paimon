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

package org.apache.paimon.management;

import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.requests.GrantPermissionRequest;
import org.apache.paimon.rest.requests.RevokePermissionRequest;
import org.apache.paimon.rest.responses.ListPermissionsResponse;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JSON and validation tests for permission management contracts. */
public class PermissionManagementJsonTest {

    private static final String ASSIGNMENT_JSON =
            "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                    + "\"table\":\"orders\"},\"access\":\"SELECT\","
                    + "\"principal\":\"analyst\","
                    + "\"expireTime\":\"2027-01-01T00:00:00Z\"}";

    private static final String COLUMN_ASSIGNMENT_JSON =
            "{\"resource\":{\"type\":\"COLUMN\",\"database\":\"sales\","
                    + "\"table\":\"orders\"},\"access\":\"SELECT\","
                    + "\"principal\":\"analyst\",\"columns\":{"
                    + "\"columnNames\":[\"id\",\"region\"]}}";

    @Test
    void testAssignmentDeserializesWithShadedAndExternalJackson() throws Exception {
        assertAssignment(RESTApi.fromJson(ASSIGNMENT_JSON, PermissionAssignment.class));
        assertAssignment(
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(ASSIGNMENT_JSON, PermissionAssignment.class));
    }

    @Test
    void testAssignmentDeserializesLowerCaseResourceType() throws Exception {
        String lowerCaseJson = ASSIGNMENT_JSON.replace("\"TABLE\"", "\"table\"");

        assertAssignment(RESTApi.fromJson(lowerCaseJson, PermissionAssignment.class));
        assertAssignment(
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(lowerCaseJson, PermissionAssignment.class));
    }

    @Test
    void testListResponseDoesNotApplyGrantValidation() throws Exception {
        String preciseExpiry = "2027-01-01T00:00:00.123456Z";
        String responseJson =
                "{\"permissions\":["
                        + ASSIGNMENT_JSON.replace("2027-01-01T00:00:00Z", preciseExpiry)
                        + "]}";

        ListPermissionsResponse shaded =
                RESTApi.fromJson(responseJson, ListPermissionsResponse.class);
        ListPermissionsResponse external =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(responseJson, ListPermissionsResponse.class);

        assertThat(shaded.getPermissions().get(0).getExpireTime()).isEqualTo(preciseExpiry);
        assertThat(external.getPermissions().get(0).getExpireTime()).isEqualTo(preciseExpiry);
        assertThatThrownBy(
                        () ->
                                new GrantPermissionRequest(
                                        tableResource(), "SELECT", "analyst", null, preciseExpiry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    @Test
    void testColumnAssignmentRoundTripsWithShadedAndExternalJackson() throws Exception {
        PermissionAssignment shaded =
                RESTApi.fromJson(COLUMN_ASSIGNMENT_JSON, PermissionAssignment.class);
        PermissionAssignment external =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(COLUMN_ASSIGNMENT_JSON, PermissionAssignment.class);

        for (PermissionAssignment assignment : Arrays.asList(shaded, external)) {
            assertThat(assignment.getResource().getType()).isEqualTo(ResourceType.COLUMN);
            assertThat(assignment.getAccess()).isEqualTo("SELECT");
            assertThat(assignment.getColumns().getColumnNames()).containsExactly("id", "region");
            assertThat(assignment.getColumns().getExcludedColumnNames()).isNull();
        }

        Map<?, ?> wire = RESTApi.fromJson(RESTApi.toJson(shaded), Map.class);
        assertThat(((Map<?, ?>) wire.get("columns")).get("columnNames"))
                .isEqualTo(Arrays.asList("id", "region"));
        Map<?, ?> externalWire =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(
                                new com.fasterxml.jackson.databind.ObjectMapper()
                                        .writeValueAsString(external),
                                Map.class);
        Map<?, ?> externalColumns = (Map<?, ?>) externalWire.get("columns");
        assertThat(externalColumns.get("columnNames")).isEqualTo(Arrays.asList("id", "region"));
        assertThat(externalColumns.get("excludedColumnNames")).isNull();
    }

    @Test
    void testGrantAndRevokeUsePrivilegeOnlyWireShapes() throws Exception {
        PermissionAssignment assignment =
                RESTApi.fromJson(ASSIGNMENT_JSON, PermissionAssignment.class);
        Map<?, ?> grant =
                RESTApi.fromJson(RESTApi.toJson(new GrantPermissionRequest(assignment)), Map.class);

        assertThat(grant.get("access")).isEqualTo("SELECT");
        assertThat(grant.containsKey("columns")).isFalse();
        assertThat(grant.containsKey("policy")).isFalse();
        assertThat(grant.containsKey("grantOption")).isFalse();

        Map<?, ?> revoke =
                RESTApi.fromJson(
                        RESTApi.toJson(
                                new RevokePermissionRequest(
                                        assignment.getResource(),
                                        assignment.getAccess(),
                                        assignment.getPrincipal())),
                        Map.class);
        assertThat(revoke.get("access")).isEqualTo("SELECT");
        assertThat(revoke.containsKey("columns")).isFalse();
        assertThat(revoke.containsKey("policy")).isFalse();
        assertThat(revoke.containsKey("policyType")).isFalse();
        assertThat(revoke.containsKey("grantOption")).isFalse();
        assertThat(revoke.containsKey("expireTime")).isFalse();
    }

    @Test
    void testPermissionRequestWithoutExpiry() throws Exception {
        String permissionJson =
                "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                        + "\"table\":\"orders\"},\"access\":\"select\","
                        + "\"principal\":\"analyst\"}";

        assertThat(RESTApi.fromJson(permissionJson, GrantPermissionRequest.class).getExpireTime())
                .isNull();
        assertThat(RESTApi.fromJson(permissionJson, RevokePermissionRequest.class).getAccess())
                .isEqualTo("SELECT");
    }

    @Test
    void testAccessAndPermissionValidation() {
        assertThat(
                        new PermissionAssignment(
                                        catalogResource(), "createdatabase", "analyst", null)
                                .getAccess())
                .isEqualTo("CREATEDATABASE");
        assertThat(
                        new PermissionAssignment(databaseResource(), "createview", "analyst", null)
                                .getAccess())
                .isEqualTo("CREATEVIEW");
        assertThat(
                        new PermissionAssignment(functionResource(), "select", "analyst", null)
                                .getAccess())
                .isEqualTo("SELECT");
        assertThat(PermissionAccess.builtIns(ResourceType.CATALOG))
                .containsExactlyInAnyOrder("ALL", "ALTER", "DROP", "GRANT", "CREATEDATABASE");
        assertThat(PermissionAccess.builtIns(ResourceType.CATALOG_ALL))
                .containsExactlyInAnyOrder(
                        "ALL",
                        "DESCRIBE",
                        "ALTER",
                        "DROP",
                        "GRANT",
                        "CREATETABLE",
                        "CREATEVIEW",
                        "CREATEFUNCTION",
                        "LIST",
                        "SELECT",
                        "UPDATE");
        assertThat(PermissionAccess.builtIns(ResourceType.DATABASE))
                .containsExactlyInAnyOrder(
                        "ALL",
                        "DESCRIBE",
                        "ALTER",
                        "DROP",
                        "GRANT",
                        "CREATETABLE",
                        "CREATEVIEW",
                        "CREATEFUNCTION",
                        "LIST");
        assertThat(PermissionAccess.builtIns(ResourceType.DATABASE_ALL))
                .containsExactlyInAnyOrder("ALL", "ALTER", "DROP", "SELECT", "UPDATE", "GRANT");
        assertThat(PermissionAccess.builtIns(ResourceType.TABLE))
                .containsExactlyInAnyOrder("ALL", "ALTER", "DROP", "SELECT", "UPDATE", "GRANT");
        assertThat(PermissionAccess.builtIns(ResourceType.VIEW))
                .containsExactlyInAnyOrder("ALL", "ALTER", "DROP", "SELECT", "GRANT");
        assertThat(PermissionAccess.builtIns(ResourceType.FUNCTION))
                .containsExactlyInAnyOrder("ALL", "ALTER", "DROP", "SELECT", "GRANT");
        assertThat(PermissionAccess.builtIns(ResourceType.COLUMN)).containsExactly("SELECT");

        PermissionColumns included = new PermissionColumns(Arrays.asList("id", "region"), null);
        assertThat(
                        new PermissionAssignment(
                                        columnResource(), "select", "analyst", included, null)
                                .getColumns())
                .isEqualTo(included);

        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        catalogResource(), "SELECT", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for CATALOG");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        databaseResource(), "SELECT", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for DATABASE");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        catalogAllResource(), "CREATEDATABASE", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for CATALOG_ALL");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        databaseAllResource(), "LIST", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for DATABASE_ALL");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(), "CREATEVIEW", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for TABLE");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        functionResource(), "UPDATE", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for FUNCTION");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        columnResource(), "UPDATE", "analyst", included, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for COLUMN");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        columnResource(), "SELECT", "analyst", null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("columns is required");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(), "SELECT", "analyst", included, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only valid for COLUMN");
        assertThatThrownBy(
                        () ->
                                new PermissionColumns(
                                        Collections.singletonList("id"),
                                        Collections.singletonList("region")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(() -> new PermissionColumns(Collections.emptyList(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be empty");
        assertThatThrownBy(() -> new PermissionColumns(Arrays.asList("id", "id"), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
        assertThatThrownBy(() -> new PermissionColumns(Collections.singletonList(" "), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty");
        for (String access :
                Arrays.asList(
                        "USE_CATALOG",
                        "CREATE_DATABASE",
                        "USE_DATABASE",
                        "CREATE_TABLE",
                        "CREATE_VIEW",
                        "CREATE_FUNCTION",
                        "INSERT",
                        "DELETE",
                        "EXECUTE",
                        "MANAGE_PERMISSIONS",
                        "vendor.example/read_sensitive")) {
            assertThatThrownBy(
                            () ->
                                    new PermissionAssignment(
                                            tableResource(), access, "analyst", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown access");
        }
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        repeat('A', PermissionAccess.MAX_LENGTH + 1),
                                        "analyst",
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(), "a/" + repeat('ß', 16), "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("after canonicalization");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        "SELECT",
                                        repeat('p', PermissionAssignment.MAX_PRINCIPAL_LENGTH + 1),
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("128");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        "SELECT",
                                        "analyst",
                                        "2027-01-01T00:00:00.000001Z"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    @Test
    void testPermissionColumnsDefensivelyCopiesItsRange() {
        java.util.List<String> source = new ArrayList<>(Arrays.asList("id", "region"));
        PermissionColumns columns = new PermissionColumns(source, null);

        source.clear();
        assertThat(columns.getColumnNames()).containsExactly("id", "region");
        assertThatThrownBy(() -> columns.getColumnNames().add("email"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPermissionListRequiresExactResourceAndBoundsPageSize() {
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.TABLE,
                                        "sales",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact target");
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.CATALOG,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        1001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 1000");

        ListPermissionsRequest databaseAccess =
                new ListPermissionsRequest(
                        ResourceType.DATABASE,
                        "sales",
                        null,
                        null,
                        null,
                        null,
                        "createview",
                        null,
                        25);
        assertThat(databaseAccess.getAccess()).isEqualTo("CREATEVIEW");
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.DATABASE,
                                        "sales",
                                        null,
                                        null,
                                        null,
                                        null,
                                        "SELECT",
                                        null,
                                        25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for DATABASE");

        assertThat(databaseAccess.withPageToken(" \t").getPageToken()).isEqualTo(" \t");
    }

    @Test
    void testResourceCanonicalizesBlankIrrelevantLocators() throws Exception {
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, "", " ", null, null);
        PermissionResource catalogAll =
                new PermissionResource(ResourceType.CATALOG_ALL, "", " ", null, null);

        assertThat(catalog).isEqualTo(catalogResource());
        assertThat(RESTApi.toJson(catalog)).isEqualTo("{\"type\":\"CATALOG\"}");
        assertThat(catalogAll).isEqualTo(catalogAllResource());
        assertThat(RESTApi.toJson(catalogAll)).isEqualTo("{\"type\":\"CATALOG_ALL\"}");
        assertThat(RESTApi.toJson(databaseAllResource()))
                .isEqualTo("{\"type\":\"DATABASE_ALL\",\"database\":\"sales\"}");
    }

    private static PermissionResource catalogResource() {
        return new PermissionResource(ResourceType.CATALOG, null, null, null, null);
    }

    private static PermissionResource catalogAllResource() {
        return new PermissionResource(ResourceType.CATALOG_ALL, null, null, null, null);
    }

    private static PermissionResource databaseResource() {
        return new PermissionResource(ResourceType.DATABASE, "sales", null, null, null);
    }

    private static PermissionResource databaseAllResource() {
        return new PermissionResource(ResourceType.DATABASE_ALL, "sales", null, null, null);
    }

    private static PermissionResource tableResource() {
        return new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null);
    }

    private static PermissionResource columnResource() {
        return new PermissionResource(ResourceType.COLUMN, "sales", "orders", null, null);
    }

    private static PermissionResource functionResource() {
        return new PermissionResource(ResourceType.FUNCTION, "sales", null, "calculate_tax", null);
    }

    private static String repeat(char value, int length) {
        char[] values = new char[length];
        Arrays.fill(values, value);
        return new String(values);
    }

    private static void assertAssignment(PermissionAssignment assignment) {
        assertThat(assignment.getResource().getType()).isEqualTo(ResourceType.TABLE);
        assertThat(assignment.getResource().getDatabase()).isEqualTo("sales");
        assertThat(assignment.getResource().getTable()).isEqualTo("orders");
        assertThat(assignment.getAccess()).isEqualTo("SELECT");
        assertThat(assignment.getPrincipal()).isEqualTo("analyst");
        assertThat(assignment.getExpireTime()).isEqualTo("2027-01-01T00:00:00Z");
    }
}
