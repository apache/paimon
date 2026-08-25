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

import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.StringType;

/** Revokes a permission by its resource identity. */
public class RevokePermissionProcedure extends BasePermissionProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("resource_type", StringType),
                ProcedureParameter.required("access", StringType),
                ProcedureParameter.required("principal", StringType),
                ProcedureParameter.optional("database", StringType),
                ProcedureParameter.optional("table", StringType),
                ProcedureParameter.optional("function", StringType),
                ProcedureParameter.optional("view", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
                    });

    private RevokePermissionProcedure(TableCatalog tableCatalog) {
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
        PermissionResource resource =
                resource(
                        resourceType,
                        args.isNullAt(3) ? null : args.getString(3),
                        args.isNullAt(4) ? null : args.getString(4),
                        args.isNullAt(5) ? null : args.getString(5),
                        args.isNullAt(6) ? null : args.getString(6));

        permissionManagement().revokePermission(resource, args.getString(1), args.getString(2));
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<RevokePermissionProcedure>() {
            @Override
            protected RevokePermissionProcedure doBuild() {
                return new RevokePermissionProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "RevokePermissionProcedure";
    }
}
