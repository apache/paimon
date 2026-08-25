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

import org.apache.paimon.PagedList;
import org.apache.paimon.management.ColumnMask;
import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.ListPoliciesRequest;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.management.PrincipalType;
import org.apache.paimon.management.RowFilter;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Lists policies attached to an exact table. */
public class ListPoliciesProcedure extends BasePolicyProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("database", StringType),
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("name", StringType),
                ProcedureParameter.optional("policy_type", StringType),
                ProcedureParameter.optional("principal_type", StringType),
                ProcedureParameter.optional("principal", StringType),
                ProcedureParameter.optional("max_results", IntegerType),
                ProcedureParameter.optional("page_token", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        field("database", StringType, false),
                        field("table", StringType, false),
                        field("name", StringType, false),
                        field("policy_type", StringType, false),
                        field("function_name", StringType, false),
                        field("on_column", StringType, true),
                        field("function_arguments_json", StringType, false),
                        field("to_principals_json", StringType, false),
                        field("except_principals_json", StringType, false),
                        field("comment", StringType, true),
                        field("next_page_token", StringType, true)
                    });

    private ListPoliciesProcedure(TableCatalog tableCatalog) {
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
        ListPoliciesRequest request =
                new ListPoliciesRequest(
                        tableResource(args.getString(0), args.getString(1)),
                        args.isNullAt(2) ? null : args.getString(2),
                        optionalEnum(
                                args.isNullAt(3) ? null : args.getString(3),
                                PolicyType.class,
                                PARAMETERS[3].name()),
                        optionalEnum(
                                args.isNullAt(4) ? null : args.getString(4),
                                PrincipalType.class,
                                PARAMETERS[4].name()),
                        args.isNullAt(5) ? null : args.getString(5),
                        args.isNullAt(7) ? null : args.getString(7),
                        args.isNullAt(6) ? null : args.getInt(6));
        PagedList<DataPolicy> page = policyManagement().listPolicies(request);
        List<DataPolicy> policies = page.getElements();
        if (policies == null || policies.isEmpty()) {
            return new InternalRow[0];
        }
        InternalRow[] rows = new InternalRow[policies.size()];
        for (int i = 0; i < policies.size(); i++) {
            DataPolicy policy = policies.get(i);
            RowFilter rowFilter = policy.getRowFilter();
            ColumnMask columnMask = policy.getColumnMask();
            rows[i] =
                    newInternalRow(
                            string(policy.getResource().getDatabase()),
                            string(policy.getResource().getTable()),
                            string(policy.getName()),
                            string(policy.type().name()),
                            string(
                                    rowFilter == null
                                            ? columnMask.getFunctionName()
                                            : rowFilter.getFunctionName()),
                            string(columnMask == null ? null : columnMask.getOnColumn()),
                            json(
                                    rowFilter == null
                                            ? columnMask.getFunctionArguments()
                                            : rowFilter.getFunctionArguments()),
                            json(policy.getToPrincipals()),
                            json(policy.getExceptPrincipals()),
                            string(policy.getComment()),
                            string(page.getNextPageToken()));
        }
        return rows;
    }

    private static StructField field(
            String name, org.apache.spark.sql.types.DataType type, boolean nullable) {
        return new StructField(name, type, nullable, Metadata.empty());
    }

    private static UTF8String string(String value) {
        return value == null ? null : UTF8String.fromString(value);
    }

    private static UTF8String json(Object value) {
        return string(JsonSerdeUtil.toFlatJson(value));
    }

    public static ProcedureBuilder builder() {
        return new Builder<ListPoliciesProcedure>() {
            @Override
            protected ListPoliciesProcedure doBuild() {
                return new ListPoliciesProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ListPoliciesProcedure";
    }
}
