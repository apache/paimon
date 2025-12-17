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
import org.apache.paimon.Snapshot;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.commands.PaimonSparkWriter;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.StringUtils;

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

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Rescale procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.rescale(table => 'databaseName.tableName', [bucket_num => 16], [partitions => 'dt=20250217,hh=08;dt=20250218,hh=08'], [where => 'dt>20250217'])
 * </code></pre>
 */
public class RescaleProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(RescaleProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("bucket_num", IntegerType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("where", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected RescaleProcedure(TableCatalog tableCatalog) {
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
        Integer bucketNum = args.isNullAt(1) ? null : args.getInt(1);
        String partitions = blank(args, 2) ? null : args.getString(2);
        String where = blank(args, 3) ? null : args.getString(3);

        checkArgument(
                partitions == null || where == null,
                "partitions and where cannot be used together.");
        String finalWhere = partitions != null ? SparkProcedureUtils.toWhere(partitions) : where;

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(table instanceof FileStoreTable);
                    FileStoreTable fileStoreTable = (FileStoreTable) table;

                    Optional<Snapshot> optionalSnapshot = fileStoreTable.latestSnapshot();
                    if (!optionalSnapshot.isPresent()) {
                        throw new IllegalArgumentException(
                                "Table "
                                        + table.fullName()
                                        + " has no snapshot, no need to rescale.");
                    }
                    Snapshot snapshot = optionalSnapshot.get();

                    // If someone commits while the rescale job is running, this commit will be
                    // lost.
                    // So we use strict mode to make sure nothing is lost.
                    Map<String, String> dynamicOptions = new HashMap<>();
                    dynamicOptions.put(
                            CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(),
                            String.valueOf(snapshot.id()));
                    fileStoreTable = fileStoreTable.copy(dynamicOptions);

                    DataSourceV2Relation relation = createRelation(tableIdent);
                    PartitionPredicate partitionPredicate =
                            SparkProcedureUtils.convertToPartitionPredicate(
                                    finalWhere,
                                    fileStoreTable.schema().logicalPartitionType(),
                                    spark(),
                                    relation);

                    if (bucketNum == null) {
                        checkArgument(
                                fileStoreTable.coreOptions().bucket() != BucketMode.POSTPONE_BUCKET,
                                "When rescaling postpone bucket tables, you must provide the resulting bucket number.");
                    }

                    execute(fileStoreTable, bucketNum, partitionPredicate, tableIdent);

                    InternalRow internalRow = newInternalRow(true);
                    return new InternalRow[] {internalRow};
                });
    }

    private void execute(
            FileStoreTable table,
            @Nullable Integer bucketNum,
            PartitionPredicate partitionPredicate,
            Identifier tableIdent) {
        DataSourceV2Relation relation = createRelation(tableIdent);

        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader = snapshotReader.withPartitionFilter(partitionPredicate);
        }
        List<DataSplit> dataSplits = snapshotReader.read().dataSplits();

        if (dataSplits.isEmpty()) {
            LOG.info("No data splits found for the specified partition. No need to rescale.");
            return;
        }

        Dataset<Row> datasetForRead =
                PaimonUtils.createDataset(
                        spark(),
                        ScanPlanHelper$.MODULE$.createNewScanPlan(
                                dataSplits.toArray(new DataSplit[0]), relation));

        Map<String, String> bucketOptions = new HashMap<>(table.options());
        if (bucketNum != null) {
            bucketOptions.put(CoreOptions.BUCKET.key(), String.valueOf(bucketNum));
        }
        FileStoreTable rescaledTable = table.copy(table.schema().copy(bucketOptions));

        PaimonSparkWriter writer = PaimonSparkWriter.apply(rescaledTable);
        writer.writeBuilder().withOverwrite();
        writer.commit(writer.write(datasetForRead));
    }

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isNullOrWhitespaceOnly(args.getString(index));
    }

    @Override
    public String description() {
        return "This procedure rescales partitions of a table by changing the bucket number.";
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<RescaleProcedure>() {
            @Override
            public RescaleProcedure doBuild() {
                return new RescaleProcedure(tableCatalog());
            }
        };
    }
}
