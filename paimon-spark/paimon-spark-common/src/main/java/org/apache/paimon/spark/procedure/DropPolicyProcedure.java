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
import org.apache.paimon.management.PolicyType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Drops one principal's row-filter or column-masking policy. */
public class DropPolicyProcedure extends BasePolicyProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("database", StringType),
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("policy_type", StringType),
                ProcedureParameter.required("principal", StringType),
                ProcedureParameter.optional("column", StringType),
                ProcedureParameter.optional("if_exists", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
                    });

    private DropPolicyProcedure(TableCatalog tableCatalog) {
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
        PermissionResource resource = tableResource(args.getString(0), args.getString(1));
        PolicyType type = enumValue(args.getString(2), PolicyType.class, PARAMETERS[2].name());
        policyManagement()
                .dropPolicy(
                        resource,
                        type,
                        args.getString(3),
                        args.isNullAt(4) ? null : args.getString(4),
                        !args.isNullAt(5) && args.getBoolean(5));
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<DropPolicyProcedure>() {
            @Override
            protected DropPolicyProcedure doBuild() {
                return new DropPolicyProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "DropPolicyProcedure";
    }
}
