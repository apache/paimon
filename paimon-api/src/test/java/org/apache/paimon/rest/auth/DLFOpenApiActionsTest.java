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

package org.apache.paimon.rest.auth;

import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.rest.ResourcePaths;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for {@link DLFOpenApiActions}. */
public class DLFOpenApiActionsTest {

    private final ResourcePaths paths = new ResourcePaths("clg-paimon-1");

    /** Each template, filled in, must come back to its own action rather than an earlier one. */
    @Test
    public void testEveryOperationResolvesToItself() {
        Set<String> actions = new HashSet<>();
        for (String[] operation : DLFOpenApiActions.operations()) {
            String path = operation[1].replaceAll("\\{[^}]+}", "x1");
            assertEquals(operation[2], DLFOpenApiActions.resolve(operation[0], path), path);
            actions.add(operation[2]);
        }
        assertEquals(53, actions.size());
    }

    @Test
    public void testClientPathsResolveToRegisteredActions() {
        assertAction("GetConfig", "GET", ResourcePaths.config());
        assertAction("ListDatabases", "GET", paths.databases());
        assertAction("CreateDatabase", "POST", paths.databases());
        assertAction("GetDatabase", "GET", paths.database("db"));
        assertAction("AlterDatabase", "POST", paths.database("db"));
        assertAction("DropDatabase", "DELETE", paths.database("db"));
        assertAction("RegisterTable", "POST", paths.registerTable("db"));
        assertAction("ListTables", "GET", paths.tables("db"));
        assertAction("CreateTable", "POST", paths.tables("db"));
        assertAction("ListTableDetails", "GET", paths.tableDetails("db"));
        assertAction("GetTable", "GET", paths.table("db", "t"));
        assertAction("AlterTable", "POST", paths.table("db", "t"));
        assertAction("DropTable", "DELETE", paths.table("db", "t"));
        assertAction("RenameTable", "POST", paths.renameTable());
        assertAction("AuthTableQuery", "POST", paths.authTable("db", "t"));
        assertAction("CommitTable", "POST", paths.commitTable("db", "t"));
        assertAction("GetTableToken", "GET", paths.tableToken("db", "t"));
        assertAction("RollbackToSnapshot", "POST", paths.rollbackTable("db", "t"));
        assertAction("GetTableSnapshot", "GET", paths.tableSnapshot("db", "t"));
        assertAction("ListSnapshots", "GET", paths.snapshots("db", "t"));
        assertAction("GetVersionSnapshot", "GET", paths.tableSnapshot("db", "t", "3"));
        assertAction("ListPartitions", "GET", paths.partitions("db", "t"));
        assertAction("CreatePartitions", "POST", paths.partitions("db", "t"));
        assertAction("DropPartitions", "POST", paths.dropPartitions("db", "t"));
        assertAction("MarkDonePartitions", "POST", paths.markDonePartitions("db", "t"));
        assertAction("ListPartitionsByNames", "POST", paths.listPartitionsByNames("db", "t"));
        assertAction("ListPartitionsByFilter", "POST", paths.listPartitionsByFilter("db", "t"));
        assertAction("ListBranches", "GET", paths.branches("db", "t"));
        assertAction("CreateBranch", "POST", paths.branches("db", "t"));
        assertAction("DropBranch", "DELETE", paths.branch("db", "t", "b"));
        assertAction("FastForwardBranch", "POST", paths.forwardBranch("db", "t", "b"));
        assertAction("ListTags", "GET", paths.tags("db", "t"));
        assertAction("CreateTag", "POST", paths.tags("db", "t"));
        assertAction("GetTag", "GET", paths.tag("db", "t", "tag"));
        assertAction("DropTag", "DELETE", paths.tag("db", "t", "tag"));
        PermissionResource table =
                new PermissionResource(ResourceType.TABLE, "db", "t", null, null);
        assertAction("ListPolicies", "GET", paths.policies(table));
        assertAction("CreatePolicy", "POST", paths.policies(table));
        assertAction("DropPolicy", "POST", paths.dropPolicy(table));
        assertAction("ListViews", "GET", paths.views("db"));
        assertAction("CreateView", "POST", paths.views("db"));
        assertAction("ListViewDetails", "GET", paths.viewDetails("db"));
        assertAction("GetView", "GET", paths.view("db", "v"));
        assertAction("AlterView", "POST", paths.view("db", "v"));
        assertAction("DropView", "DELETE", paths.view("db", "v"));
        assertAction("RenameView", "POST", paths.renameView());
        assertAction("ListFunctions", "GET", paths.functions("db"));
        assertAction("CreateFunction", "POST", paths.functions("db"));
        assertAction("GetFunction", "GET", paths.function("db", "f"));
        assertAction("AlterFunction", "POST", paths.function("db", "f"));
        assertAction("DropFunction", "DELETE", paths.function("db", "f"));
        assertAction("ListPermissionAssignments", "GET", paths.permissions());
        assertAction("GrantPermissionAssignment", "POST", paths.grantPermission());
        assertAction("RevokePermissionAssignment", "POST", paths.revokePermission());
    }

    /** Names match whole segments, so one spelled like a literal still resolves. */
    @Test
    public void testNamesSpelledLikeLiterals() {
        assertAction("GetDatabase", "GET", paths.database("tables"));
        assertAction("GetTable", "GET", paths.table("db", "token"));
        assertAction("DropBranch", "DELETE", paths.branch("db", "t", "forward"));
        assertAction("GetTag", "GET", paths.tag("db", "t", "tags"));
        assertAction("GetTableToken", "GET", paths.tableToken("config", "t$snapshots"));
        assertAction("GetTable", "GET", paths.table("a/b", "c d"));
        assertAction("ListDatabases", "GET", new ResourcePaths("rename").databases());
    }

    @Test
    public void testUnregisteredRequestsHaveNoAction() {
        assertNull(DLFOpenApiActions.resolve("GET", paths.tables()));
        assertNull(DLFOpenApiActions.resolve("GET", paths.table("tbl-1")));
        assertNull(DLFOpenApiActions.resolve("GET", paths.schemas("db", "t")));
        assertNull(DLFOpenApiActions.resolve("PUT", paths.table("db", "t")));
        assertNull(DLFOpenApiActions.resolve("GET", ResourcePaths.config() + "/"));
        assertNull(DLFOpenApiActions.resolve("GET", "/v1//databases"));
        assertNull(DLFOpenApiActions.resolve(null, ResourcePaths.config()));
        assertNull(DLFOpenApiActions.resolve("GET", null));
    }

    @Test
    public void testMethodIsCaseInsensitive() {
        assertAction("GetConfig", "get", ResourcePaths.config());
    }

    private static void assertAction(String expected, String method, String path) {
        assertEquals(expected, DLFOpenApiActions.resolve(method, path), method + " " + path);
    }
}
