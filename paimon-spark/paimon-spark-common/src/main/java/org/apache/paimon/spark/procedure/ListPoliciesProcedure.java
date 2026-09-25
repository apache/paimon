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
import org.apache.paimon.management.RowFilter;

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
                ProcedureParameter.optional("policy_type", StringType),
                ProcedureParameter.optional("principal", StringType),
                ProcedureParameter.optional("column", StringType),
                ProcedureParameter.optional("max_results", IntegerType),
                ProcedureParameter.optional("page_token", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        field("database", StringType, false),
                        field("table", StringType, false),
                        field("policy_type", StringType, false),
                        field("principal", StringType, false),
                        field("predicate_json", StringType, true),
                        field("on_column", StringType, true),
                        field("transform_json", StringType, true),
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
                        optionalEnum(
                                args.isNullAt(2) ? null : args.getString(2),
                                PolicyType.class,
                                PARAMETERS[2].name()),
                        args.isNullAt(3) ? null : args.getString(3),
                        args.isNullAt(4) ? null : args.getString(4),
                        args.isNullAt(6) ? null : args.getString(6),
                        args.isNullAt(5) ? null : args.getInt(5));
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
                            string(policy.type().name()),
                            string(policy.getPrincipal()),
                            string(rowFilter == null ? null : rowFilter.getPredicate()),
                            string(columnMask == null ? null : columnMask.getOnColumn()),
                            string(columnMask == null ? null : columnMask.getTransform()),
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
