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
import org.apache.paimon.rest.requests.PolicyRequest;
import org.apache.paimon.rest.requests.RevokePermissionRequest;

import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JSON and validation tests for permission and data-policy management contracts. */
public class PermissionManagementJsonTest {

    private static final String ASSIGNMENT_JSON =
            "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                    + "\"table\":\"orders\"},\"scope\":\"SELF\",\"access\":\"select\","
                    + "\"principal\":{\"type\":\"ROLE\",\"id\":\"analyst\"},"
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
                ASSIGNMENT_JSON
                        .replace("\"TABLE\"", "\"table\"")
                        .replace("\"SELF\"", "\"self\"")
                        .replace("\"ROLE\"", "\"role\"");

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
                        + "\"principal\":{\"type\":\"ROLE\",\"id\":\"analyst\"}}";

        assertThat(RESTApi.fromJson(permissionJson, GrantPermissionRequest.class).getScope())
                .isEqualTo(PermissionScope.SELF);
        assertThat(RESTApi.fromJson(permissionJson, RevokePermissionRequest.class).getScope())
                .isEqualTo(PermissionScope.SELF);

        String policyJson =
                "{\"name\":\"filter\",\"rowFilter\":{\"functionName\":"
                        + "\"security.filter\"},\"toPrincipals\":[{\"type\":\"ROLE\","
                        + "\"id\":\"analyst\"}]}";
        DataPolicy policy =
                RESTApi.fromJson(policyJson, PolicyRequest.class).policy(tableResource());
        assertThat(policy.getExceptPrincipals()).isEmpty();
    }

    @Test
    void testPoliciesAreNamedFunctionBasedResources() throws Exception {
        String json =
                "{\"resource\":{\"type\":\"TABLE\",\"database\":\"sales\","
                        + "\"table\":\"orders\"},\"name\":\"mask_email\","
                        + "\"columnMask\":{\"functionName\":\"security.mask_email\","
                        + "\"onColumn\":\"email\",\"functionArguments\":[{\"column\":\"region\"},"
                        + "{\"constant\":\"CN\"}]},\"toPrincipals\":[{\"type\":\"GROUP\","
                        + "\"id\":\"analysts\"}],\"exceptPrincipals\":[{\"type\":\"USER\","
                        + "\"id\":\"admin\"}]}";

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
        assertThat(policy.getToPrincipals())
                .containsExactly(new PrincipalRef(PrincipalType.GROUP, "analysts"));
        assertThat(externalPolicy.getColumnMask().getFunctionName())
                .isEqualTo("security.mask_email");
        assertThat(externalPolicy.getColumnMask().getFunctionArguments().get(1).getConstant())
                .isEqualTo("CN");

        PolicyRequest policyRequest = new PolicyRequest(policy);
        assertThat(policyRequest.isRetrySafe()).isFalse();
        Map<?, ?> request = RESTApi.fromJson(RESTApi.toJson(policyRequest), Map.class);
        assertThat(request.containsKey("resource")).isFalse();
        assertThat(request.get("name")).isEqualTo("mask_email");
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
                                        role("analyst"),
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
                                        role("analyst"),
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
                                        role("analyst"),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown access");
        assertThat(
                        new PermissionAssignment(
                                        tableResource(),
                                        PermissionScope.SELF,
                                        "vendor.example/read_sensitive",
                                        role("analyst"),
                                        null,
                                        null)
                                .getAccess())
                .isEqualTo("VENDOR.EXAMPLE/READ_SENSITIVE");
        assertThatThrownBy(() -> new PolicyArgument("region", "CN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThat(PolicyArgument.constant("").getConstant()).isEmpty();
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        tableResource(),
                                        "filter",
                                        new RowFilter("security.filter", null),
                                        new ColumnMask("security.mask", "region", null),
                                        Collections.singletonList(role("analyst")),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(
                        () ->
                                new DataPolicy(
                                        catalogResource(),
                                        "mask",
                                        null,
                                        new ColumnMask("security.mask", "email", null),
                                        Collections.singletonList(role("analyst")),
                                        null,
                                        null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TABLE");
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
                                        null,
                                        "USE_CATALOG",
                                        false,
                                        null,
                                        25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid for assignments on DATABASE");
    }

    @Test
    void testPolicyDefensivelyCopiesCollections() {
        ArrayList<PrincipalRef> principals =
                new ArrayList<>(Collections.singletonList(role("analyst")));
        ArrayList<PolicyArgument> arguments =
                new ArrayList<>(Arrays.asList(PolicyArgument.column("region")));
        DataPolicy policy =
                DataPolicy.rowFilter(
                        tableResource(),
                        "filter",
                        new RowFilter("security.filter", arguments),
                        principals,
                        null,
                        null);

        principals.add(role("admin"));
        arguments.add(PolicyArgument.constant("CN"));

        assertThat(policy.getToPrincipals()).containsExactly(role("analyst"));
        assertThat(policy.getRowFilter().getFunctionArguments()).hasSize(1);
    }

    @Test
    void testPolicyAcceptsListsThatRejectNullLookups() {
        PrincipalRef principal = role("analyst");
        List<PrincipalRef> principals =
                new AbstractList<PrincipalRef>() {
                    @Override
                    public PrincipalRef get(int index) {
                        if (index != 0) {
                            throw new IndexOutOfBoundsException();
                        }
                        return principal;
                    }

                    @Override
                    public int size() {
                        return 1;
                    }

                    @Override
                    public boolean contains(Object value) {
                        if (value == null) {
                            throw new NullPointerException("null lookups are not supported");
                        }
                        return super.contains(value);
                    }
                };

        DataPolicy policy =
                DataPolicy.rowFilter(
                        tableResource(),
                        "filter",
                        new RowFilter("security.filter", Collections.emptyList()),
                        principals,
                        null,
                        null);

        assertThat(policy.getToPrincipals()).containsExactly(principal);
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

    private static PrincipalRef role(String id) {
        return new PrincipalRef(PrincipalType.ROLE, id);
    }

    private static void assertAssignment(PermissionAssignment assignment) {
        assertThat(assignment.getResource().getType()).isEqualTo(ResourceType.TABLE);
        assertThat(assignment.getResource().getDatabase()).isEqualTo("sales");
        assertThat(assignment.getResource().getTable()).isEqualTo("orders");
        assertThat(assignment.getScope()).isEqualTo(PermissionScope.SELF);
        assertThat(assignment.getAccess()).isEqualTo("SELECT");
        assertThat(assignment.getPrincipal()).isEqualTo(role("analyst"));
        assertThat(assignment.getExpireTime()).isEqualTo("2027-01-01T00:00:00Z");
        assertThat(assignment.getInheritedFrom()).isNull();
    }
}
