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

import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;

import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test for {@link ResourcePaths}. */
public class ResourcePathsTest {

    @Test
    public void testUrlEncode() {
        String database = "test_db";
        String objectName = "test_table$snapshot";
        ResourcePaths resourcePaths = new ResourcePaths("paimon");
        assertEquals(
                "/v1/paimon/databases/test_db/tables/test_table%24snapshot",
                resourcePaths.table(database, objectName));

        resourcePaths = new ResourcePaths("paimon/aaaa");
        assertEquals(
                "/v1/paimon%2Faaaa/databases/test_db/tables/test_table%24snapshot",
                resourcePaths.table(database, objectName));
    }

    @Test
    public void testPermissionManagementUsesPrefix() {
        ResourcePaths resourcePaths = new ResourcePaths("catalog/id");
        assertEquals("/v1/catalog%2Fid/permissions", resourcePaths.permissions());
        assertEquals("/v1/catalog%2Fid/permissions/grant", resourcePaths.grantPermission());
        assertEquals("/v1/catalog%2Fid/permissions/revoke", resourcePaths.revokePermission());
    }

    @Test
    public void testCommitTransactionUsesPrefix() {
        ResourcePaths resourcePaths = new ResourcePaths("catalog/id");
        assertEquals("/v1/catalog%2Fid/transactions/commit", resourcePaths.commitTransaction());
    }

    @Test
    public void testPoliciesAreNestedUnderAttachmentResource() {
        ResourcePaths paths = new ResourcePaths("catalog/id");
        PermissionResource catalog =
                new PermissionResource(ResourceType.CATALOG, null, null, null, null);
        PermissionResource database =
                new PermissionResource(ResourceType.DATABASE, "sales db", null, null, null);
        PermissionResource table =
                new PermissionResource(ResourceType.TABLE, "sales db", "orders/all", null, null);

        assertThrows(IllegalArgumentException.class, () -> paths.policies(catalog));
        assertThrows(IllegalArgumentException.class, () -> paths.policies(database));
        assertEquals(
                "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall/policies",
                paths.policies(table));
        assertEquals(
                "/v1/catalog%2Fid/databases/sales+db/tables/orders%2Fall/policies/drop",
                paths.dropPolicy(table));
    }
}
