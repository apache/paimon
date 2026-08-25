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
import org.apache.paimon.management.PolicyArgument;
import org.apache.paimon.management.PolicyIdentity;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.management.RowFilter;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Shared typed argument conversion for table policy procedures. */
abstract class BasePolicyProcedure extends BasePermissionProcedure {

    protected BasePolicyProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    protected static org.apache.spark.sql.types.DataType functionArgumentArrayType() {
        return DataTypes.createArrayType(StringType);
    }

    protected static DataPolicy policy(
            String database,
            String table,
            PolicyType policyType,
            String principal,
            String functionName,
            @Nullable String onColumn,
            @Nullable ArrayData functionArguments) {
        PermissionResource resource = tableResource(database, table);
        List<PolicyArgument> arguments = arguments(functionArguments);
        if (policyType == PolicyType.ROW_FILTER) {
            checkArgument(isBlank(onColumn), "ROW_FILTER policy cannot specify on_column.");
            return DataPolicy.rowFilter(
                    resource, new RowFilter(functionName, arguments), principal);
        }
        return DataPolicy.columnMask(
                resource, new ColumnMask(functionName, onColumn, arguments), principal);
    }

    protected static PolicyIdentity policyIdentity(
            String database,
            String table,
            PolicyType type,
            String principal,
            @Nullable String column) {
        return new PolicyIdentity(tableResource(database, table), type, principal, column);
    }

    protected static PermissionResource tableResource(String database, String table) {
        return resource(ResourceType.TABLE, database, table, null, null);
    }

    private static List<PolicyArgument> arguments(@Nullable ArrayData data) {
        if (data == null) {
            return Collections.emptyList();
        }
        List<PolicyArgument> arguments = new ArrayList<>(data.numElements());
        for (int i = 0; i < data.numElements(); i++) {
            checkArgument(!data.isNullAt(i), "function_arguments cannot contain null.");
            UTF8String value = data.getUTF8String(i);
            String encoded = value.toString();
            boolean constant = encoded.regionMatches(true, 0, "constant:", 0, "constant:".length());
            String[] argument =
                    split(
                            encoded,
                            "function_arguments",
                            "column:value or constant:value",
                            constant);
            if ("column".equalsIgnoreCase(argument[0])) {
                arguments.add(PolicyArgument.column(argument[1]));
            } else {
                checkArgument(
                        "constant".equalsIgnoreCase(argument[0]),
                        "function_arguments entries must start with column: or constant:.");
                arguments.add(PolicyArgument.constant(argument[1]));
            }
        }
        return arguments;
    }

    private static String[] split(
            String encoded, String argument, String expected, boolean allowEmptyValue) {
        int delimiter = encoded.indexOf(':');
        checkArgument(
                delimiter > 0 && (allowEmptyValue || delimiter < encoded.length() - 1),
                "%s entry '%s' must use %s.",
                argument,
                encoded,
                expected);
        return new String[] {encoded.substring(0, delimiter), encoded.substring(delimiter + 1)};
    }
}
