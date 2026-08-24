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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** JSON compatibility tests for the permission management contract. */
public class PermissionManagementJsonTest {

    private static final String PERMISSION_JSON =
            "{\"resourceType\":\"COLUMN\",\"catalog\":\"catalog-name\","
                    + "\"database\":\"sales\",\"table\":\"orders\","
                    + "\"columns\":{\"columnNames\":[\"id\",\"amount\"]},"
                    + "\"access\":\"SELECT\",\"principal\":\"role:analyst\","
                    + "\"expireTime\":\"2027-01-01T00:00:00Z\"}";

    @Test
    void testPermissionDeserializesWithShadedAndExternalJackson() throws Exception {
        Permission shaded = RESTApi.fromJson(PERMISSION_JSON, Permission.class);
        Permission external =
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .readValue(PERMISSION_JSON, Permission.class);

        assertPermission(shaded);
        assertPermission(external);
    }

    @Test
    void testGrantAndRevokeUseFlatWireShapes() throws Exception {
        Permission permission = RESTApi.fromJson(PERMISSION_JSON, Permission.class);
        Map<?, ?> grant =
                RESTApi.fromJson(RESTApi.toJson(new GrantPermissionRequest(permission)), Map.class);
        assertThat(grant.get("resourceType")).isEqualTo("COLUMN");
        assertThat(grant.get("catalog")).isEqualTo("catalog-name");
        assertThat(grant.get("database")).isEqualTo("sales");
        assertThat(grant.get("table")).isEqualTo("orders");
        assertThat(grant.get("access")).isEqualTo("SELECT");
        assertThat(grant.get("principal")).isEqualTo("role:analyst");
        assertThat(grant.get("expireTime")).isEqualTo("2027-01-01T00:00:00Z");
        assertThat(grant.containsKey("permission")).isFalse();

        Map<?, ?> revoke =
                RESTApi.fromJson(
                        RESTApi.toJson(new RevokePermissionRequest(permission)), Map.class);
        assertThat(revoke.get("resourceType")).isEqualTo("COLUMN");
        assertThat(revoke.get("catalog")).isEqualTo("catalog-name");
        assertThat(revoke.get("database")).isEqualTo("sales");
        assertThat(revoke.get("table")).isEqualTo("orders");
        assertThat(revoke.get("access")).isEqualTo("SELECT");
        assertThat(revoke.get("principal")).isEqualTo("role:analyst");
        assertThat(revoke.containsKey("expireTime")).isFalse();
        assertThat(revoke.containsKey("rowFilter")).isFalse();
        assertThat(revoke.containsKey("columnMasking")).isFalse();
    }

    @Test
    void testRowFilterAndColumnMaskUseExpressionPayloads() throws Exception {
        String json =
                "{\"resourceType\":\"ROW_FILTER\",\"database\":\"sales\","
                        + "\"table\":\"orders\",\"rowFilter\":{\"expression\":\"region = 'cn'\","
                        + "\"predicate\":\"predicate-json\"},\"columnMasking\":{\"email\":{"
                        + "\"expression\":\"UPPER(email)\",\"transform\":\"transform-json\"}},"
                        + "\"access\":\"ROW_FILTER\",\"principal\":\"role:analyst\"}";

        Permission permission = RESTApi.fromJson(json, Permission.class);
        assertThat(permission.getRowFilter().getExpression()).isEqualTo("region = 'cn'");
        assertThat(permission.getRowFilter().getPredicate()).isEqualTo("predicate-json");
        assertThat(permission.getColumnMasking().get("email").getExpression())
                .isEqualTo("UPPER(email)");
        assertThat(permission.getColumnMasking().get("email").getTransform())
                .isEqualTo("transform-json");
    }

    private static void assertPermission(Permission permission) {
        assertThat(permission.getResourceType()).isEqualTo(ResourceType.COLUMN);
        assertThat(permission.getCatalog()).isEqualTo("catalog-name");
        assertThat(permission.getDatabase()).isEqualTo("sales");
        assertThat(permission.getTable()).isEqualTo("orders");
        assertThat(permission.getColumns().getColumnNames()).containsExactly("id", "amount");
        assertThat(permission.getColumns().getExcludedColumnNames()).isNull();
        assertThat(permission.getAccess()).isEqualTo("SELECT");
        assertThat(permission.getPrincipal()).isEqualTo("role:analyst");
        assertThat(permission.getExpireTime()).isEqualTo("2027-01-01T00:00:00Z");
    }
}
