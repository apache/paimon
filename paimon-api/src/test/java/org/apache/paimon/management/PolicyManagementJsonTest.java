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
import org.apache.paimon.rest.requests.PolicyRequest;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** JSON and validation tests for data-policy management contracts. */
public class PolicyManagementJsonTest {

    private static final String PREDICATE_JSON =
            "{\"kind\":\"LEAF\",\"transform\":{\"name\":\"FIELD_REF\","
                    + "\"fieldRef\":{\"index\":0,\"name\":\"region\",\"type\":\"STRING\"}},"
                    + "\"function\":\"EQUAL\",\"literals\":[\"APAC\"]}";
    private static final String TRANSFORM_JSON =
            "{\"name\":\"CONCAT\",\"inputs\":[{\"index\":0,\"name\":\"region\","
                    + "\"type\":\"STRING\"},\"****\"]}";

    @Test
    void testPolicyDefinitionsRoundTripWithShadedAndExternalJackson() throws Exception {
        DataPolicy policy =
                DataPolicy.columnMask(
                        tableResource(), new ColumnMask("email", TRANSFORM_JSON), "analyst");
        String json = RESTApi.toJson(policy);

        DataPolicy shaded = RESTApi.fromJson(json, DataPolicy.class);
        DataPolicy external =
                new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, DataPolicy.class);
        for (DataPolicy roundTrip : Arrays.asList(shaded, external)) {
            assertThat(roundTrip.type()).isEqualTo(PolicyType.COLUMN_MASKING);
            assertThat(roundTrip.getResource()).isEqualTo(tableResource());
            assertThat(roundTrip.getColumnMask().getOnColumn()).isEqualTo("email");
            assertThat(roundTrip.getColumnMask().getTransform()).isEqualTo(TRANSFORM_JSON);
            assertThat(roundTrip.getPrincipal()).isEqualTo("analyst");
        }

        PolicyRequest request = new PolicyRequest(policy);
        assertThat(request.isRetrySafe()).isFalse();
        Map<?, ?> wire = RESTApi.fromJson(RESTApi.toJson(request), Map.class);
        assertThat(wire.keySet()).containsExactlyInAnyOrder("columnMask", "principal");
        assertThat(request.policy(tableResource()).getResource()).isEqualTo(tableResource());
    }

    @Test
    void testRowFilterRoundTrip() throws Exception {
        DataPolicy policy =
                DataPolicy.rowFilter(tableResource(), new RowFilter(PREDICATE_JSON), "analyst");

        DataPolicy roundTrip = RESTApi.fromJson(RESTApi.toJson(policy), DataPolicy.class);
        assertThat(roundTrip.type()).isEqualTo(PolicyType.ROW_FILTER);
        assertThat(roundTrip.getRowFilter().getPredicate()).isEqualTo(PREDICATE_JSON);
        assertThat(roundTrip.getColumnMask()).isNull();
    }

    @Test
    void testPolicyValidationAndPayloadBounds() {
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
                                        new ColumnMask("email", TRANSFORM_JSON),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
        assertThatThrownBy(
                        () ->
                                DataPolicy.columnMask(
                                        catalogResource(),
                                        new ColumnMask("email", TRANSFORM_JSON),
                                        "analyst"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TABLE");
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
    void testDropPolicyRequestRoundTripAndIdentityValidation() throws Exception {
        DropPolicyRequest request =
                new DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", "email");
        DropPolicyRequest roundTrip =
                RESTApi.fromJson(RESTApi.toJson(request), DropPolicyRequest.class);

        assertThat(roundTrip.getType()).isEqualTo(PolicyType.COLUMN_MASKING);
        assertThat(roundTrip.getPrincipal()).isEqualTo("analyst");
        assertThat(roundTrip.getColumn()).isEqualTo("email");
        assertThatThrownBy(() -> new DropPolicyRequest(PolicyType.COLUMN_MASKING, "analyst", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column is required");
        assertThatThrownBy(() -> new DropPolicyRequest(PolicyType.ROW_FILTER, "analyst", "email"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot contain a column");
    }

    @Test
    void testListPoliciesValidationAndOpaquePageToken() {
        ListPoliciesRequest request =
                new ListPoliciesRequest(
                        tableResource(), PolicyType.COLUMN_MASKING, "analyst", "email", null, 25);

        assertThat(request.withPageToken(" \t").getPageToken()).isEqualTo(" \t");
        assertThatThrownBy(
                        () ->
                                new ListPoliciesRequest(
                                        tableResource(), null, null, "email", null, 25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("COLUMN_MASKING");
        assertThatThrownBy(
                        () ->
                                new ListPoliciesRequest(
                                        tableResource(), null, null, null, null, 1001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 1000");
        assertThatThrownBy(
                        () ->
                                new ListPoliciesRequest(
                                        catalogResource(), null, null, null, null, 25))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TABLE");
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
}
