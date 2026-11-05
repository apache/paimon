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

import org.apache.paimon.management.DataPolicy;
import org.apache.paimon.management.PolicyManagement.PolicyAlreadyExistException;
import org.apache.paimon.management.PolicyType;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Creates a table row-filter or column-mask policy. */
public class CreatePolicyProcedure extends BasePolicyProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("database", StringType),
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("policy_type", StringType),
                ProcedureParameter.required("principal", StringType),
                ProcedureParameter.optional("predicate_json", StringType),
                ProcedureParameter.optional("on_column", StringType),
                ProcedureParameter.optional("transform_json", StringType),
                ProcedureParameter.optional("filter_sql", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
                    });

    private CreatePolicyProcedure(TableCatalog tableCatalog) {
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
        String database = args.getString(0);
        String table = args.getString(1);
        PolicyType policyType =
                enumValue(args.getString(2), PolicyType.class, PARAMETERS[2].name());
        String predicate = args.isNullAt(4) ? null : args.getString(4);
        String filterSql = args.isNullAt(7) ? null : args.getString(7);
        if (policyType == PolicyType.ROW_FILTER) {
            checkArgument(
                    isBlank(predicate) != isBlank(filterSql),
                    "ROW_FILTER policy requires exactly one of predicate_json or filter_sql.");
            if (!isBlank(filterSql)) {
                Identifier identifier = Identifier.of(new String[] {database}, table);
                SparkTable sparkTable = loadSparkTable(identifier);
                predicate =
                        JsonSerdeUtil.toFlatJson(
                                ExpressionUtils.convertPolicyFilter(
                                        spark(),
                                        createRelation(identifier, sparkTable),
                                        sparkTable.getTable().rowType(),
                                        filterSql));
            }
        } else {
            checkArgument(isBlank(filterSql), "COLUMN_MASKING policy cannot specify filter_sql.");
        }
        DataPolicy policy =
                policy(
                        database,
                        table,
                        policyType,
                        args.getString(3),
                        predicate,
                        args.isNullAt(5) ? null : args.getString(5),
                        args.isNullAt(6) ? null : args.getString(6));
        try {
            policyManagement().createPolicy(policy);
        } catch (PolicyAlreadyExistException e) {
            throw new RuntimeException(e);
        }
        return new InternalRow[] {newInternalRow(true)};
    }

    public static ProcedureBuilder builder() {
        return new Builder<CreatePolicyProcedure>() {
            @Override
            protected CreatePolicyProcedure doBuild() {
                return new CreatePolicyProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "CreatePolicyProcedure";
    }
}
