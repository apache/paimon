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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.PagedList;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.ResourceType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Lists direct permissions on an exact resource or explicit descendant scope. Usage:
 *
 * <pre><code>
 *  CALL sys.list_permissions(
 *    resource_type =&gt; 'TABLE',
 *    `database` =&gt; 'sales',
 *    `table` =&gt; 'orders')
 * </code></pre>
 */
public class ListPermissionsProcedure extends BasePermissionProcedure {

    public static final String IDENTIFIER = "list_permissions";

    private static final String OUTPUT_TYPE =
            "ROW<resource_type STRING, `database` STRING, `table` STRING, `function` STRING, "
                    + "`view` STRING, access STRING, principal STRING, "
                    + "column_names ARRAY<STRING>, excluded_column_names ARRAY<STRING>, "
                    + "expire_time STRING, next_page_token STRING>";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "resource_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "function", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "view", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "principal",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(name = "access", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "max_results", type = @DataTypeHint("INT"), isOptional = true),
                @ArgumentHint(
                        name = "page_token",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public @DataTypeHint(OUTPUT_TYPE) Row[] call(
            ProcedureContext procedureContext,
            String resourceType,
            String database,
            String table,
            String function,
            String view,
            String principal,
            String access,
            Integer maxResults,
            String pageToken) {
        PagedList<PermissionAssignment> page =
                permissionManagement()
                        .listPermissions(
                                new ListPermissionsRequest(
                                        enumValue(
                                                resourceType, ResourceType.class, "resource_type"),
                                        emptyToNull(database),
                                        emptyToNull(table),
                                        emptyToNull(function),
                                        emptyToNull(view),
                                        emptyToNull(principal),
                                        emptyToNull(access),
                                        emptyToNull(pageToken),
                                        maxResults));
        List<PermissionAssignment> assignments = page.getElements();
        if (assignments == null || assignments.isEmpty()) {
            return new Row[0];
        }

        Row[] rows = new Row[assignments.size()];
        for (int i = 0; i < assignments.size(); i++) {
            PermissionAssignment assignment = assignments.get(i);
            PermissionColumns columns = assignment.getColumns();
            rows[i] =
                    Row.of(
                            assignment.getResource().getType().name(),
                            assignment.getResource().getDatabase(),
                            assignment.getResource().getTable(),
                            assignment.getResource().getFunction(),
                            assignment.getResource().getView(),
                            assignment.getAccess(),
                            assignment.getPrincipal(),
                            toArray(columns == null ? null : columns.getColumnNames()),
                            toArray(columns == null ? null : columns.getExcludedColumnNames()),
                            assignment.getExpireTime(),
                            page.getNextPageToken());
        }
        return rows;
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
