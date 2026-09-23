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

import org.apache.paimon.annotation.VisibleForTesting;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Maps a REST request to the POP action it is registered under in DlfNext 2026-01-18. */
final class DLFOpenApiActions {

    /** Method, path template and action; a {@code {name}} segment matches any one segment. */
    private static final String[][] OPERATIONS = {
        {"GET", "/v1/config", "GetConfig"},
        {"GET", "/v1/{prefix}/databases", "ListDatabases"},
        {"POST", "/v1/{prefix}/databases", "CreateDatabase"},
        {"GET", "/v1/{prefix}/databases/{database}", "GetDatabase"},
        {"POST", "/v1/{prefix}/databases/{database}", "AlterDatabase"},
        {"DELETE", "/v1/{prefix}/databases/{database}", "DropDatabase"},
        {"POST", "/v1/{prefix}/databases/{database}/register", "RegisterTable"},
        {"GET", "/v1/{prefix}/databases/{database}/tables", "ListTables"},
        {"POST", "/v1/{prefix}/databases/{database}/tables", "CreateTable"},
        {"GET", "/v1/{prefix}/databases/{database}/table-details", "ListTableDetails"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}", "GetTable"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}", "AlterTable"},
        {"DELETE", "/v1/{prefix}/databases/{database}/tables/{table}", "DropTable"},
        {"POST", "/v1/{prefix}/tables/rename", "RenameTable"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/auth", "AuthTableQuery"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/commit", "CommitTable"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/token", "GetTableToken"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/rollback", "RollbackToSnapshot"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/snapshot", "GetTableSnapshot"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/snapshots", "ListSnapshots"},
        {
            "GET",
            "/v1/{prefix}/databases/{database}/tables/{table}/snapshots/{version}",
            "GetVersionSnapshot"
        },
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/partitions", "ListPartitions"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/partitions", "CreatePartitions"},
        {
            "POST",
            "/v1/{prefix}/databases/{database}/tables/{table}/partitions/drop",
            "DropPartitions"
        },
        {
            "POST",
            "/v1/{prefix}/databases/{database}/tables/{table}/partitions/mark",
            "MarkDonePartitions"
        },
        {
            "POST",
            "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-names",
            "ListPartitionsByNames"
        },
        {
            "POST",
            "/v1/{prefix}/databases/{database}/tables/{table}/partitions/list-by-filter",
            "ListPartitionsByFilter"
        },
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/branches", "ListBranches"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/branches", "CreateBranch"},
        {
            "DELETE",
            "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}",
            "DropBranch"
        },
        {
            "POST",
            "/v1/{prefix}/databases/{database}/tables/{table}/branches/{branch}/forward",
            "FastForwardBranch"
        },
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/tags", "ListTags"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/tags", "CreateTag"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}", "GetTag"},
        {"DELETE", "/v1/{prefix}/databases/{database}/tables/{table}/tags/{tag}", "DropTag"},
        {"GET", "/v1/{prefix}/databases/{database}/tables/{table}/policies", "ListPolicies"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/policies", "CreatePolicy"},
        {"POST", "/v1/{prefix}/databases/{database}/tables/{table}/policies/drop", "DropPolicy"},
        {"GET", "/v1/{prefix}/databases/{database}/views", "ListViews"},
        {"POST", "/v1/{prefix}/databases/{database}/views", "CreateView"},
        {"GET", "/v1/{prefix}/databases/{database}/view-details", "ListViewDetails"},
        {"GET", "/v1/{prefix}/databases/{database}/views/{view}", "GetView"},
        {"POST", "/v1/{prefix}/databases/{database}/views/{view}", "AlterView"},
        {"DELETE", "/v1/{prefix}/databases/{database}/views/{view}", "DropView"},
        {"POST", "/v1/{prefix}/views/rename", "RenameView"},
        {"GET", "/v1/{prefix}/databases/{database}/functions", "ListFunctions"},
        {"POST", "/v1/{prefix}/databases/{database}/functions", "CreateFunction"},
        {"GET", "/v1/{prefix}/databases/{database}/functions/{function}", "GetFunction"},
        {"POST", "/v1/{prefix}/databases/{database}/functions/{function}", "AlterFunction"},
        {"DELETE", "/v1/{prefix}/databases/{database}/functions/{function}", "DropFunction"},
        {"GET", "/v1/{prefix}/permissions", "ListPermissionAssignments"},
        {"POST", "/v1/{prefix}/permissions/grant", "GrantPermissionAssignment"},
        {"POST", "/v1/{prefix}/permissions/revoke", "RevokePermissionAssignment"},
    };

    private static final Map<String, List<Operation>> BY_METHOD = index();

    private DLFOpenApiActions() {}

    /** Returns the action for the request, or null when no operation is registered for it. */
    @Nullable
    static String resolve(@Nullable String method, @Nullable String resourcePath) {
        if (method == null || resourcePath == null) {
            return null;
        }
        List<Operation> candidates = BY_METHOD.get(method.toUpperCase(Locale.ROOT));
        if (candidates == null) {
            return null;
        }
        String[] segments = resourcePath.split("/", -1);
        for (Operation operation : candidates) {
            if (operation.matches(segments)) {
                return operation.action;
            }
        }
        return null;
    }

    /** Every registered operation as {method, path template, action}. */
    @VisibleForTesting
    static List<String[]> operations() {
        List<String[]> copy = new ArrayList<>();
        for (String[] operation : OPERATIONS) {
            copy.add(operation.clone());
        }
        return copy;
    }

    private static Map<String, List<Operation>> index() {
        Map<String, List<Operation>> byMethod = new HashMap<>();
        for (String[] operation : OPERATIONS) {
            byMethod.computeIfAbsent(operation[0], k -> new ArrayList<>())
                    .add(new Operation(operation[1].split("/", -1), operation[2]));
        }
        return Collections.unmodifiableMap(byMethod);
    }

    private static final class Operation {
        private final String[] template;
        private final String action;

        private Operation(String[] template, String action) {
            this.template = template;
            this.action = action;
        }

        private boolean matches(String[] segments) {
            if (segments.length != template.length) {
                return false;
            }
            for (int i = 0; i < template.length; i++) {
                boolean variable = template[i].startsWith("{");
                if (variable ? segments[i].isEmpty() : !template[i].equals(segments[i])) {
                    return false;
                }
            }
            return true;
        }
    }
}
