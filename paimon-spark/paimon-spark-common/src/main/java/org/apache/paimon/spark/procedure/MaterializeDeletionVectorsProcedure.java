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

import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Procedure which physically applies deletion vectors and assigns new row IDs. */
public class MaterializeDeletionVectorsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional("where", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, false, Metadata.empty())
                    });

    private MaterializeDeletionVectorsProcedure(TableCatalog tableCatalog) {
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
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String partitions = blank(args, 1) ? null : args.getString(1);
        String options = args.isNullAt(2) ? null : args.getString(2);
        String where = blank(args, 3) ? null : args.getString(3);
        checkArgument(
                partitions == null || where == null,
                "partitions and where cannot be used together.");

        return modifySparkTable(
                tableIdent,
                sparkTable -> {
                    checkArgument(sparkTable.getTable() instanceof FileStoreTable);
                    FileStoreTable table = (FileStoreTable) sparkTable.getTable();
                    HashMap<String, String> dynamicOptions = new HashMap<>();
                    ProcedureUtils.putAllOptions(dynamicOptions, options);
                    table = table.copy(dynamicOptions);

                    checkArgument(
                            table.bucketMode() == BucketMode.BUCKET_UNAWARE
                                    && table.coreOptions().dataEvolutionEnabled(),
                            "Materializing deletion vectors only supports unaware-bucket data evolution tables.");
                    checkArgument(
                            table.coreOptions().deletionVectorsEnabled(),
                            "Materializing deletion vectors requires deletion vectors to be enabled.");

                    DataSourceV2Relation relation = createRelation(tableIdent, sparkTable);
                    PartitionPredicate partitionPredicate;
                    if (partitions != null) {
                        partitionPredicate =
                                SparkProcedureUtils.convertPartitionsToPartitionPredicate(
                                        partitions, table, spark());
                    } else {
                        partitionPredicate =
                                SparkProcedureUtils.convertToPartitionPredicate(
                                        where,
                                        table.schema().logicalPartitionType(),
                                        spark(),
                                        relation);
                    }
                    CompactProcedure.executeDataEvolutionCompaction(
                            table,
                            partitionPredicate,
                            null,
                            new JavaSparkContext(spark().sparkContext()),
                            spark(),
                            true);
                    return new InternalRow[] {newInternalRow(true)};
                });
    }

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isNullOrWhitespaceOnly(args.getString(index));
    }

    @Override
    public String description() {
        return "Physically apply deletion vectors and assign new row IDs.";
    }

    public static ProcedureBuilder builder() {
        return new Builder<MaterializeDeletionVectorsProcedure>() {
            @Override
            public MaterializeDeletionVectorsProcedure doBuild() {
                return new MaterializeDeletionVectorsProcedure(tableCatalog());
            }
        };
    }
}
