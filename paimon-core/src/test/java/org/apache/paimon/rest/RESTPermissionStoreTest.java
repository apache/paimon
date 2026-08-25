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

package org.apache.paimon.rest;

import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PermissionScope;
import org.apache.paimon.management.PrincipalRef;
import org.apache.paimon.management.PrincipalType;
import org.apache.paimon.management.ResourceType;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests atomic replacement and effective-view filtering in {@link RESTPermissionStore}. */
class RESTPermissionStoreTest {

    private static final PrincipalRef ANALYST = new PrincipalRef(PrincipalType.ROLE, "analyst");

    @Test
    void testConcurrentGrantReplacesTheSameIdentity() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource table = tableResource();

        IntStream.range(0, 1000)
                .parallel()
                .forEach(
                        i ->
                                store.put(
                                        new PermissionAssignment(
                                                table,
                                                PermissionScope.SELF,
                                                "SELECT",
                                                ANALYST,
                                                Instant.ofEpochSecond(i).toString(),
                                                null)));

        assertThat(store.list(table, tableParameters(), false)).hasSize(1);
    }

    @Test
    void testScopeFilterAppliesToInheritedEffectiveView() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, null, null, null, null);
        store.put(
                new PermissionAssignment(
                        catalog, PermissionScope.DESCENDANTS, "SELECT", ANALYST, null, null));
        Map<String, String> parameters = tableParameters();
        parameters.put("scope", "SELF");

        List<PermissionAssignment> assignments = store.list(tableResource(), parameters, true);

        assertThat(assignments).hasSize(1);
        assertThat(assignments.get(0).getScope()).isEqualTo(PermissionScope.SELF);
        assertThat(assignments.get(0).getInheritedFrom()).isEqualTo(catalog);
    }

    @Test
    void testIncludingInheritedAlsoKeepsDirectDescendantsAssignment() {
        RESTPermissionStore store = new RESTPermissionStore();
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, null, null, null, null);
        store.put(
                new PermissionAssignment(
                        catalog, PermissionScope.DESCENDANTS, "SELECT", ANALYST, null, null));
        Map<String, String> parameters = new HashMap<>();
        parameters.put("resourceType", "CATALOG");

        List<PermissionAssignment> assignments = store.list(catalog, parameters, true);

        assertThat(assignments).hasSize(1);
        assertThat(assignments.get(0).getScope()).isEqualTo(PermissionScope.DESCENDANTS);
        assertThat(assignments.get(0).getInheritedFrom()).isNull();
    }

    private static PermissionResource tableResource() {
        return new PermissionResource(ResourceType.TABLE, "sales", "orders", null, null);
    }

    private static Map<String, String> tableParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("resourceType", "TABLE");
        parameters.put("database", "sales");
        parameters.put("table", "orders");
        return parameters;
    }
}
