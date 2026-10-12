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

import org.apache.paimon.management.ResourceType;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

/**
 * Grants a permission through a REST catalog. Usage:
 *
 * <pre><code>
 *  CALL sys.grant_permission(
 *    resource_type =&gt; 'TABLE',
 *    access =&gt; 'SELECT',
 *    principal =&gt; 'analyst',
 *    `database` =&gt; 'sales',
 *    `table` =&gt; 'orders')
 * </code></pre>
 */
public class GrantPermissionProcedure extends BasePermissionProcedure {

    public static final String IDENTIFIER = "grant_permission";

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "resource_type", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "access", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "principal", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "database", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "function", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "view", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(
                        name = "expire_time",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "column_names",
                        type = @DataTypeHint("STRING"),
                        isOptional = true),
                @ArgumentHint(
                        name = "excluded_column_names",
                        type = @DataTypeHint("STRING"),
                        isOptional = true)
            })
    public @DataTypeHint("ROW<result BOOLEAN>") Row[] call(
            ProcedureContext procedureContext,
            String resourceType,
            String access,
            String principal,
            String database,
            String table,
            String function,
            String view,
            String expireTime,
            String columnNames,
            String excludedColumnNames) {
        permissionManagement()
                .grantPermission(
                        assignment(
                                enumValue(resourceType, ResourceType.class, "resource_type"),
                                access,
                                principal,
                                database,
                                table,
                                function,
                                view,
                                columns(columnNames, excludedColumnNames),
                                expireTime));
        return new Row[] {Row.of(true)};
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }
}
