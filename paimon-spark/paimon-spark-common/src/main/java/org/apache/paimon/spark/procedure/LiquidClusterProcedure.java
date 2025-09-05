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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.cluster.ClusterManager;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.spark.commands.PaimonSparkWriter;
import org.apache.paimon.spark.sort.TableSorter;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.PaimonUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** doc. */
public class LiquidClusterProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(LiquidClusterProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("isFull", BooleanType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    private LiquidClusterProcedure(TableCatalog tableCatalog) {
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
        boolean isFull = !args.isNullAt(1) && args.getBoolean(1);
        String options = args.isNullAt(2) ? null : args.getString(2);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(table instanceof FileStoreTable);
                    CoreOptions coreOptions = ((FileStoreTable) table).coreOptions();

                    DataSourceV2Relation relation = createRelation(tableIdent);

                    HashMap<String, String> dynamicOptions = new HashMap<>();
                    ProcedureUtils.putIfNotEmpty(
                            dynamicOptions, CoreOptions.WRITE_ONLY.key(), "false");
                    ProcedureUtils.putAllOptions(dynamicOptions, options);
                    table = table.copy(dynamicOptions);
                    InternalRow internalRow =
                            newInternalRow(
                                    execute(
                                            (FileStoreTable) table,
                                            isFull,
                                            coreOptions.clusteringStrategy(
                                                    table.partitionKeys().size()),
                                            coreOptions.liquidClusterColumns(),
                                            relation));
                    return new InternalRow[] {internalRow};
                });
    }

    @Override
    public String description() {
        return "This procedure execute liquid cluster action on paimon table.";
    }

    private boolean execute(
            FileStoreTable table,
            boolean isFull,
            CoreOptions.OrderType curve,
            List<String> clusterColumns,
            DataSourceV2Relation relation) {
        BucketMode bucketMode = table.bucketMode();

        checkArgument(
                bucketMode == BucketMode.BUCKET_UNAWARE,
                "Liquid cluster only support unaware-bucket append-only table yet.");

        sortCompactUnAwareBucketTable(table, curve, clusterColumns, isFull, relation);
        return true;
    }

    private void sortCompactUnAwareBucketTable(
            FileStoreTable table,
            CoreOptions.OrderType orderType,
            List<String> clusterColumns,
            boolean fullCompaction,
            DataSourceV2Relation relation) {
        ClusterManager clusterManager = new ClusterManager(table);
        Map<BinaryRow, CompactUnit> compactUnits = clusterManager.prepareForCluster(fullCompaction);

        // generate splits for each partition
        Map<BinaryRow, DataSplit[]> partitionSplits =
                compactUnits.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry ->
                                                clusterManager
                                                        .toSplits(
                                                                entry.getKey(),
                                                                entry.getValue().files())
                                                        .toArray(new DataSplit[0])));

        TableSorter sorter = TableSorter.getSorter(table, orderType, clusterColumns);
        Dataset<Row> datasetForWrite =
                partitionSplits.values().stream()
                        .map(
                                split -> {
                                    Dataset<Row> dataset =
                                            PaimonUtils.createDataset(
                                                    spark(),
                                                    ScanPlanHelper$.MODULE$.createNewScanPlan(
                                                            split, relation));
                                    return sorter.sort(dataset);
                                })
                        .reduce(Dataset::union)
                        .orElse(null);
        if (datasetForWrite != null) {
            PaimonSparkWriter writer = PaimonSparkWriter.apply(table);
            // Use dynamic partition overwrite
            writer.writeBuilder().withOverwrite();
            writer.commit(writer.write(datasetForWrite));
        }
    }
}
