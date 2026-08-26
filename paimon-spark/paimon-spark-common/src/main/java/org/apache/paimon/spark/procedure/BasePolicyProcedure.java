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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;

import org.apache.spark.sql.connector.catalog.TableCatalog;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared typed argument conversion for table policy procedures. */
abstract class BasePolicyProcedure extends BasePermissionProcedure {

    protected BasePolicyProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    protected static DataPolicy policy(
            String database,
            String table,
            PolicyType policyType,
            String principal,
            @Nullable String predicate,
            @Nullable String onColumn,
            @Nullable String transform) {
        PermissionResource resource = tableResource(database, table);
        if (policyType == PolicyType.ROW_FILTER) {
            checkArgument(isBlank(onColumn), "ROW_FILTER policy cannot specify on_column.");
            checkArgument(isBlank(transform), "ROW_FILTER policy cannot specify transform.");
            return DataPolicy.rowFilter(resource, new RowFilter(predicate), principal);
        }
        checkArgument(isBlank(predicate), "COLUMN_MASKING policy cannot specify predicate.");
        return DataPolicy.columnMask(resource, new ColumnMask(onColumn, transform), principal);
    }

    protected static PermissionResource tableResource(String database, String table) {
        return resource(ResourceType.TABLE, database, table, null, null);
    }
}
