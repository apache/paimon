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
import org.apache.paimon.spark.commands.PaimonSparkWriter;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.ParameterUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Procedure to compact chain table. Usage:
 *
 * <pre><code>
 *  -- Compact chain table, overwrite default is false
 *  CALL sys.compact_chain_table(table => 'db.table', partition => 'dt="20250810",hour="22"', [overwrite => true])
 * </code></pre>
 */
public class CompactChainTableProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CompactChainTableProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("partition", StringType),
                ProcedureParameter.optional("overwrite", BooleanType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", BooleanType, false, Metadata.empty())
                    });

    protected CompactChainTableProcedure(TableCatalog tableCatalog) {
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
        String partitionStr = args.getString(1);
        boolean overwrite = !args.isNullAt(2) && args.getBoolean(2);
        checkArgument(
                partitionStr == null || !partitionStr.isEmpty(),
                "Partition string cannot be empty");

        return modifyPaimonTable(
                tableIdent,
                t -> {
                    checkArgument(
                            t instanceof FallbackReadFileStoreTable,
                            "Table %s is not a chain table",
                            tableIdent);
                    FallbackReadFileStoreTable table = (FallbackReadFileStoreTable) t;
                    checkArgument(
                            table.fallback() instanceof ChainGroupReadTable,
                            "Table %s is not a chain table",
                            tableIdent);
                    DataSourceV2Relation relation = createRelation(tableIdent);
                    boolean success =
                            execute(
                                    (ChainGroupReadTable) table.fallback(),
                                    relation,
                                    partitionStr,
                                    overwrite);
                    return new InternalRow[] {newInternalRow(success)};
                });
    }

    private boolean execute(
            ChainGroupReadTable table,
            DataSourceV2Relation relation,
            String partitionStr,
            boolean overwrite) {
        String partition = SparkProcedureUtils.toWhere(partitionStr);
        FileStoreTable snapshotTable = table.wrapped();

        // Check if target partition already exists in snapshot branch
        PartitionPredicate snapshotPartitionPredicate =
                SparkProcedureUtils.convertToPartitionPredicate(
                        partition,
                        snapshotTable.schema().logicalPartitionType(),
                        spark(),
                        relation);
        boolean partitionExists =
                !snapshotTable
                        .newScan()
                        .withPartitionFilter(snapshotPartitionPredicate)
                        .plan()
                        .splits()
                        .isEmpty();

        FallbackReadFileStoreTable.FallbackReadScan scan =
                (FallbackReadFileStoreTable.FallbackReadScan) table.newScan();
        PartitionPredicate partitionPredicate =
                SparkProcedureUtils.convertToPartitionPredicate(
                        partition, table.schema().logicalPartitionType(), spark(), relation);
        if (partitionExists) {
            if (overwrite) {
                scan.withPartitionFilter(
                        SparkProcedureUtils.convertToPartitionPredicate(
                                "!(" + partition + ")",
                                table.schema().logicalPartitionType(),
                                spark(),
                                relation),
                        partitionPredicate);
                LOG.info("Found existing partition {}, will overwrite it.", partition);
            } else {
                LOG.info(
                        "Partition {} already exists in snapshot branch, skipping compaction.",
                        partitionStr);
                return false;
            }
        } else {
            scan.withPartitionFilter(partitionPredicate);
        }

        Dataset<Row> datasetForWrite =
                scan.plan().splits().stream()
                        .map(
                                split -> {
                                    DataSplit[] dataSplits =
                                            ((ChainSplit) split)
                                                    .dataSplits()
                                                    .toArray(new DataSplit[0]);
                                    return PaimonUtils.createDataset(
                                            spark(),
                                            ScanPlanHelper$.MODULE$.createNewScanPlan(
                                                    dataSplits, relation));
                                })
                        .reduce(Dataset::union)
                        .orElse(null);

        if (datasetForWrite != null) {
            PaimonSparkWriter writer = PaimonSparkWriter.apply(snapshotTable);
            if (partitionExists) {
                writer.writeBuilder().withOverwrite();
            }
            Map<String, String> targetPartition = ParameterUtils.getPartitions(partitionStr).get(0);
            for (Map.Entry<String, String> entry : targetPartition.entrySet()) {
                datasetForWrite =
                        datasetForWrite.withColumn(
                                entry.getKey(), functions.expr(entry.getValue()));
            }
            writer.commit(writer.write(datasetForWrite));
            LOG.info("Successfully compacted partition {} to snapshot branch.", partitionStr);
            return true;
        } else {
            LOG.warn("Table {} is empty, skip compaction.", table);
            return false;
        }
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CompactChainTableProcedure>() {
            @Override
            public CompactChainTableProcedure doBuild() {
                return new CompactChainTableProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "Compact chain table by merging snapshot + delta into target snapshot.";
    }
}
