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
import org.apache.paimon.rest.requests.DropPolicyRequest;
import org.apache.paimon.rest.requests.GrantPermissionRequest;
import org.apache.paimon.rest.requests.PolicyRequest;
import org.apache.paimon.rest.requests.RevokePermissionRequest;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JSON and validation tests for permission and data-policy management contracts. */
public class PermissionManagementJsonTest {

    private static final String PREDICATE_JSON =
            "{\"kind\":\"LEAF\",\"transform\":{\"name\":\"FIELD_REF\","
                    + "\"fieldRef\":{\"index\":0,\"name\":\"region\",\"type\":\"STRING\"}},"
                    + "\"function\":\"EQUAL\",\"literals\":[\"APAC\"]}";
    private static final String TRANSFORM_JSON =
            "{\"name\":\"CONCAT\",\"inputs\":[{\"index\":0,\"name\":\"region\","
                    + "\"type\":\"STRING\"},\"****\"]}";

    private static final String ASSIGNMENT_JSON =
            "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                    + "\"table\":\"orders\"},\"access\":\"select\","
                    + "\"principal\":\"analyst\","
                    + "\"expireTime\":\"2027-01-01T00:00:00Z\"}";

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
                                        PermissionIdentity.fromAssignment(assignment))),
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

        String policyJson =
                RESTApi.toJson(
                        new PolicyRequest(
                                DataPolicy.rowFilter(
                                        tableResource(),
                                        new RowFilter(PREDICATE_JSON),
                                        "analyst")));
        DataPolicy policy =
                RESTApi.fromJson(policyJson, PolicyRequest.class).policy(tableResource());
        assertThat(policy.getRowFilter().getPredicate()).isEqualTo(PREDICATE_JSON);
    }

    @Test
    void testPoliciesArePrincipalScopedPaimonDefinitions() throws Exception {
        String json =
                RESTApi.toJson(
                        DataPolicy.columnMask(
                                tableResource(),
                                new ColumnMask("email", TRANSFORM_JSON),
                                "analysts"));

        DataPolicy policy = RESTApi.fromJson(json, DataPolicy.class);
        DataPolicy externalPolicy =
                new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, DataPolicy.class);
        assertThat(policy.type()).isEqualTo(PolicyType.COLUMN_MASKING);
        assertThat(policy.getRowFilter()).isNull();
        assertThat(policy.getColumnMask().getOnColumn()).isEqualTo("email");
        assertThat(policy.getColumnMask().getTransform()).isEqualTo(TRANSFORM_JSON);
        assertThat(policy.getPrincipal()).isEqualTo("analysts");
        assertThat(externalPolicy.getColumnMask().getTransform()).isEqualTo(TRANSFORM_JSON);

        PolicyRequest policyRequest = new PolicyRequest(policy);
        assertThat(policyRequest.isRetrySafe()).isFalse();
        Map<?, ?> request = RESTApi.fromJson(RESTApi.toJson(policyRequest), Map.class);
        assertThat(request.containsKey("resource")).isFalse();
        assertThat(request.get("principal")).isEqualTo("analysts");
        assertThat(request.containsKey("type")).isFalse();
        assertThat(request.get("columnMask")).isInstanceOf(Map.class);
        Map<?, ?> external =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(
                                new com.fasterxml.jackson.databind.ObjectMapper()
                                        .writeValueAsString(policy),
                                Map.class);
        assertThat(external.containsKey("type")).isFalse();
        assertThat(((Map<?, ?>) external.get("columnMask")).keySet())
                .containsExactlyInAnyOrder("onColumn", "transform");
    }

    @Test
    void testAccessAndPolicyValidation() {
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        catalogResource(), "SELECT", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid");
        assertThatThrownBy(
                        () -> new PermissionAssignment(catalogResource(), "ALL", "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown access");
        assertThat(
                        new PermissionAssignment(
                                        tableResource(),
                                        "vendor.example/read_sensitive",
                                        "analyst",
                                        null)
                                .getAccess())
                .isEqualTo("VENDOR.EXAMPLE/READ_SENSITIVE");
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
        assertThatThrownBy(() -> new RowFilter(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("predicate");
        assertThatThrownBy(() -> new ColumnMask("email", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transform");
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        tableResource(),
                                        new RowFilter(PREDICATE_JSON),
                                        new ColumnMask("region", TRANSFORM_JSON),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        catalogResource(),
                                        null,
                                        new ColumnMask("email", TRANSFORM_JSON),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TABLE");
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

        ListPermissionsRequest catalogAccess =
                new ListPermissionsRequest(
                        ResourceType.CATALOG,
                        null,
                        null,
                        null,
                        null,
                        null,
                        "use_catalog",
                        null,
                        25);
        assertThat(catalogAccess.getAccess()).isEqualTo("USE_CATALOG");
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.DATABASE,
                                        "sales",
                                        null,
                                        null,
                                        null,
                                        null,
                                        "USE_CATALOG",
                                        null,
                                        25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for DATABASE");
    }

    @Test
    void testPolicyDefinitionsBoundUtf8PayloadSize() {
        assertThatThrownBy(() -> new RowFilter(repeat('p', RowFilter.MAX_PREDICATE_BYTES + 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8 bytes");
        assertThatThrownBy(
                        () ->
                                new ColumnMask(
                                        "email", repeat('t', ColumnMask.MAX_TRANSFORM_BYTES + 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("UTF-8 bytes");
    }

    @Test
    void testPolicyIdentityAndDropRequestRoundTrip() throws Exception {
        PolicyIdentity identity =
                new PolicyIdentity(tableResource(), PolicyType.COLUMN_MASKING, "analyst", "email");
        DropPolicyRequest request = new DropPolicyRequest(identity);
        DropPolicyRequest roundTrip =
                RESTApi.fromJson(RESTApi.toJson(request), DropPolicyRequest.class);

        assertThat(roundTrip.identity(tableResource()).getType())
                .isEqualTo(PolicyType.COLUMN_MASKING);
        assertThat(roundTrip.identity(tableResource()).getPrincipal()).isEqualTo("analyst");
        assertThat(roundTrip.identity(tableResource()).getColumn()).isEqualTo("email");
        assertThatThrownBy(
                        () ->
                                new PolicyIdentity(
                                        tableResource(),
                                        PolicyType.COLUMN_MASKING,
                                        "analyst",
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column is required");
    }

    @Test
    void testResourceCanonicalizesBlankIrrelevantLocators() throws Exception {
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, "", " ", null, null);

        assertThat(catalog).isEqualTo(catalogResource());
        assertThat(RESTApi.toJson(catalog)).isEqualTo("{\"type\":\"CATALOG\"}");
    }

    private static PermissionResource catalogResource() {
        return new PermissionResource(ResourceType.CATALOG, null, null, null, null);
    }

    private static PermissionResource tableResource() {
        return new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null);
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
