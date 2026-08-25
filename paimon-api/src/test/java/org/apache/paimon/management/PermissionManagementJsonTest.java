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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JSON and validation tests for permission and data-policy management contracts. */
public class PermissionManagementJsonTest {

    private static final String ASSIGNMENT_JSON =
            "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                    + "\"table\":\"orders\"},\"scope\":\"SELF\",\"access\":\"select\","
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
    void testAssignmentDeserializesLowerCaseDiscriminators() throws Exception {
        String lowerCaseJson =
                ASSIGNMENT_JSON.replace("\"TABLE\"", "\"table\"").replace("\"SELF\"", "\"self\"");

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

        assertThat(grant.get("scope")).isEqualTo("SELF");
        assertThat(grant.get("access")).isEqualTo("SELECT");
        assertThat(grant.containsKey("columns")).isFalse();
        assertThat(grant.containsKey("policy")).isFalse();
        assertThat(grant.containsKey("grantOption")).isFalse();
        assertThat(grant.containsKey("inheritedFrom")).isFalse();

        Map<?, ?> revoke =
                RESTApi.fromJson(
                        RESTApi.toJson(
                                new RevokePermissionRequest(
                                        PermissionIdentity.fromAssignment(assignment))),
                        Map.class);
        assertThat(revoke.get("scope")).isEqualTo("SELF");
        assertThat(revoke.get("access")).isEqualTo("SELECT");
        assertThat(revoke.containsKey("columns")).isFalse();
        assertThat(revoke.containsKey("policy")).isFalse();
        assertThat(revoke.containsKey("policyType")).isFalse();
        assertThat(revoke.containsKey("grantOption")).isFalse();
        assertThat(revoke.containsKey("expireTime")).isFalse();
    }

    @Test
    void testOptionalRequestFieldsUseDocumentedDefaults() throws Exception {
        String permissionJson =
                "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                        + "\"table\":\"orders\"},\"access\":\"select\","
                        + "\"principal\":\"analyst\"}";

        assertThat(RESTApi.fromJson(permissionJson, GrantPermissionRequest.class).getScope())
                .isEqualTo(PermissionScope.SELF);
        assertThat(RESTApi.fromJson(permissionJson, RevokePermissionRequest.class).getScope())
                .isEqualTo(PermissionScope.SELF);

        String policyJson =
                "{\"rowFilter\":{\"functionName\":\"security.filter\"},"
                        + "\"principal\":\"analyst\"}";
        DataPolicy policy =
                RESTApi.fromJson(policyJson, PolicyRequest.class).policy(tableResource());
        assertThat(policy.getRowFilter().getFunctionArguments()).isEmpty();
    }

    @Test
    void testPoliciesArePrincipalScopedFunctionBasedResources() throws Exception {
        String json =
                "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                        + "\"table\":\"orders\"},"
                        + "\"columnMask\":{\"functionName\":\"security.mask_email\","
                        + "\"onColumn\":\"email\",\"functionArguments\":[{\"column\":\"region\"},"
                        + "{\"constant\":\"CN\"}]},\"principal\":\"analysts\"}";

        DataPolicy policy = RESTApi.fromJson(json, DataPolicy.class);
        DataPolicy externalPolicy =
                new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, DataPolicy.class);
        assertThat(policy.type()).isEqualTo(PolicyType.COLUMN_MASKING);
        assertThat(policy.getRowFilter()).isNull();
        assertThat(policy.getColumnMask().getFunctionName()).isEqualTo("security.mask_email");
        assertThat(policy.getColumnMask().getOnColumn()).isEqualTo("email");
        assertThat(policy.getColumnMask().getFunctionArguments().get(0).getColumn())
                .isEqualTo("region");
        assertThat(policy.getColumnMask().getFunctionArguments().get(1).getConstant())
                .isEqualTo("CN");
        assertThat(policy.getPrincipal()).isEqualTo("analysts");
        assertThat(externalPolicy.getColumnMask().getFunctionName())
                .isEqualTo("security.mask_email");
        assertThat(externalPolicy.getColumnMask().getFunctionArguments().get(1).getConstant())
                .isEqualTo("CN");

        PolicyRequest policyRequest = new PolicyRequest(policy);
        assertThat(policyRequest.isRetrySafe()).isFalse();
        Map<?, ?> request = RESTApi.fromJson(RESTApi.toJson(policyRequest), Map.class);
        assertThat(request.containsKey("resource")).isFalse();
        assertThat(request.get("principal")).isEqualTo("analysts");
        assertThat(request.containsKey("type")).isFalse();
        assertThat(request.containsKey("scope")).isFalse();
        assertThat(request.get("columnMask")).isInstanceOf(Map.class);
        Map<?, ?> external =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(
                                new com.fasterxml.jackson.databind.ObjectMapper()
                                        .writeValueAsString(policy),
                                Map.class);
        assertThat(external.containsKey("type")).isFalse();
    }

    @Test
    void testAccessAndPolicyValidation() {
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.DESCENDANTS,
                                        "SELECT",
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DESCENDANTS");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        catalogResource(),
                                        PermissionScope.SELF,
                                        "SELECT",
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        catalogResource(),
                                        PermissionScope.SELF,
                                        "ALL",
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown access");
        assertThat(
                        new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        "vendor.example/read_sensitive",
                                        "analyst",
                                        null,
                                        null)
                                .getAccess())
                .isEqualTo("VENDOR.EXAMPLE/READ_SENSITIVE");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.DESCENDANTS,
                                        "vendor.example/read_sensitive",
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("DESCENDANTS");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        repeat('A', PermissionAccess.MAX_LENGTH + 1),
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("32");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        "a/" + repeat('ß', 16),
                                        "analyst",
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("after canonicalization");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        "SELECT",
                                        repeat('p', PermissionAssignment.MAX_PRINCIPAL_LENGTH + 1),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("128");
        assertThatThrownBy(() -> new PolicyArgument("region", "CN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThat(PolicyArgument.constant("").getConstant()).isEmpty();
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        tableResource(),
                                        new RowFilter("security.filter", null),
                                        new ColumnMask("security.mask", "region", null),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        catalogResource(),
                                        null,
                                        new ColumnMask("security.mask", "email", null),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TABLE");
        assertThatThrownBy(
                        () ->
                                new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        "SELECT",
                                        "analyst",
                                        "2027-01-01T00:00:00.000001Z",
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("millisecond");
    }

    @Test
    void testPermissionListRequiresExactResourceAndBoundsPageSize() {
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.TABLE,
                                        null,
                                        "sales",
                                        null,
                                        null,
                                        null,
                                        null,
                                        null,
                                        false,
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
                                        false,
                                        null,
                                        1001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 1000");

        ListPermissionsRequest descendantsAccess =
                new ListPermissionsRequest(
                        ResourceType.CATALOG,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        "select",
                        false,
                        null,
                        25);
        assertThat(descendantsAccess.getAccess()).isEqualTo("SELECT");
        assertThatThrownBy(
                        () ->
                                new ListPermissionsRequest(
                                        ResourceType.DATABASE,
                                        null,
                                        "sales",
                                        null,
                                        null,
                                        null,
                                        null,
                                        "USE_CATALOG",
                                        false,
                                        null,
                                        25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for assignments on DATABASE");
    }

    @Test
    void testPolicyDefensivelyCopiesFunctionArguments() {
        ArrayList<PolicyArgument> arguments =
                new ArrayList<>(Arrays.asList(PolicyArgument.column("region")));
        DataPolicy policy =
                DataPolicy.rowFilter(
                        tableResource(), new RowFilter("security.filter", arguments), "analyst");

        arguments.add(PolicyArgument.constant("CN"));

        assertThat(policy.getPrincipal()).isEqualTo("analyst");
        assertThat(policy.getRowFilter().getFunctionArguments()).hasSize(1);
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
    void testInheritedAssignmentIdentityUsesAttachmentSource() {
        PermissionResource database =
                new PermissionResource(ResourceType.DATABASE, "sales", null, null, null);
        PermissionAssignment inherited =
                new PermissionAssignment(
                        tableResource(), PermissionScope.SELF, "SELECT", "analyst", null, database);

        PermissionIdentity identity = PermissionIdentity.fromAssignment(inherited);
        assertThat(identity.getResource()).isEqualTo(database);
        assertThat(identity.getScope()).isEqualTo(PermissionScope.DESCENDANTS);
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
        assertThat(assignment.getScope()).isEqualTo(PermissionScope.SELF);
        assertThat(assignment.getAccess()).isEqualTo("SELECT");
        assertThat(assignment.getPrincipal()).isEqualTo("analyst");
        assertThat(assignment.getExpireTime()).isEqualTo("2027-01-01T00:00:00Z");
        assertThat(assignment.getInheritedFrom()).isNull();
    }
}
