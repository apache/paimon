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

import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PrincipalType;
import org.apache.paimon.management.ResourceType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** Grants a permission through a REST catalog. */
public class GrantPermissionProcedure extends BasePermissionProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("resource_type", StringType),
                ProcedureParameter.required("access", StringType),
                ProcedureParameter.required("principal_type", StringType),
                ProcedureParameter.required("principal", StringType),
                ProcedureParameter.optional("database", StringType),
                ProcedureParameter.optional("table", StringType),
                ProcedureParameter.optional("function", StringType),
                ProcedureParameter.optional("view", StringType),
                ProcedureParameter.optional("scope", StringType),
                ProcedureParameter.optional("expire_time", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "result",
                                org.apache.spark.sql.types.DataTypes.BooleanType,
                                false,
                                Metadata.empty())
                    });

    private GrantPermissionProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        ResourceType resourceType =
                enumValue(args.getString(0), ResourceType.class, PARAMETERS[0].name());
        PermissionAssignment assignment =
                assignment(
                        resourceType,
                        args.getString(1),
                        enumValue(args.getString(2), PrincipalType.class, PARAMETERS[2].name()),
                        args.getString(3),
                        args.isNullAt(4) ? null : args.getString(4),
                        args.isNullAt(5) ? null : args.getString(5),
                        args.isNullAt(6) ? null : args.getString(6),
                        args.isNullAt(7) ? null : args.getString(7),
                        args.isNullAt(8) ? null : args.getString(8),
                        args.isNullAt(9) ? null : args.getString(9));

        permissionManagement().grantPermission(assignment);
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<GrantPermissionProcedure>() {
            @Override
            protected GrantPermissionProcedure doBuild() {
                return new GrantPermissionProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "GrantPermissionProcedure";
    }
}
