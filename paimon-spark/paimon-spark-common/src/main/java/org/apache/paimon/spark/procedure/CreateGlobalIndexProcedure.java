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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderFactoryUtils;
import org.apache.paimon.spark.globalindex.GlobalIndexTopoBuilder;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.Range;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_ROW_COUNT_PER_SHARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** Procedure to build global index files via Spark. */
public class CreateGlobalIndexProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CreateGlobalIndexProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", DataTypes.StringType),
                ProcedureParameter.required("index_column", DataTypes.StringType),
                ProcedureParameter.required("index_type", DataTypes.StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("options", DataTypes.StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected CreateGlobalIndexProcedure(TableCatalog tableCatalog) {
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
    public String description() {
        return "Create global index files for a given column.";
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String column = args.getString(1);
        String indexType = args.getString(2).toLowerCase(Locale.ROOT).trim();
        String partitions =
                (args.isNullAt(3) || StringUtils.isNullOrWhitespaceOnly(args.getString(3)))
                        ? null
                        : args.getString(3);
        String optionString = args.isNullAt(4) ? null : args.getString(4);

        String finalWhere = partitions != null ? SparkProcedureUtils.toWhere(partitions) : null;

        LOG.info("Starting to build index for table " + tableIdent + " WHERE: " + finalWhere);

        return modifyPaimonTable(
                tableIdent,
                t -> {
                    try {
                        checkArgument(
                                t instanceof FileStoreTable,
                                "Only FileStoreTable supports global index creation.");
                        FileStoreTable table = (FileStoreTable) t;
                        checkArgument(
                                table.coreOptions().rowTrackingEnabled(),
                                "Table '%s' must enable 'row-tracking.enabled=true' before creating global index.",
                                tableIdent);

                        RowType rowType = table.rowType();
                        checkArgument(
                                rowType.containsField(column),
                                "Column '%s' does not exist in table '%s'.",
                                column,
                                tableIdent);
                        DataSourceV2Relation relation = createRelation(tableIdent);
                        PartitionPredicate partitionPredicate =
                                SparkProcedureUtils.convertToPartitionPredicate(
                                        finalWhere,
                                        table.schema().logicalPartitionType(),
                                        spark(),
                                        relation);

                        DataField indexField = rowType.getField(column);
                        RowType projectedRowType =
                                rowType.project(Collections.singletonList(column));
                        RowType readRowType = SpecialFields.rowTypeWithRowId(projectedRowType);

                        HashMap<String, String> parsedOptions = new HashMap<>();
                        ProcedureUtils.putAllOptions(parsedOptions, optionString);
                        Options userOptions = Options.fromMap(parsedOptions);
                        Options tableOptions = new Options(table.options());
                        long rowsPerShard =
                                tableOptions
                                        .getOptional(GLOBAL_INDEX_ROW_COUNT_PER_SHARD)
                                        .orElse(GLOBAL_INDEX_ROW_COUNT_PER_SHARD.defaultValue());
                        checkArgument(
                                rowsPerShard > 0,
                                "Option 'global-index.row-count-per-shard' must be greater than 0.");

                        // Step 1: generate splits for each partition&&shard
                        Map<BinaryRow, List<IndexedSplit>> splits =
                                split(table, partitionPredicate, rowsPerShard);

                        List<CommitMessage> indexResults;
                        // Step 2: build index by certain index system
                        GlobalIndexTopoBuilder topoBuildr =
                                GlobalIndexBuilderFactoryUtils.createTopoBuilder(indexType);
                        if (topoBuildr != null) {
                            indexResults =
                                    topoBuildr.buildIndex(
                                            new JavaSparkContext(spark().sparkContext()),
                                            table,
                                            splits,
                                            indexType,
                                            readRowType,
                                            indexField,
                                            userOptions);
                        } else {
                            indexResults =
                                    buildIndex(
                                            table,
                                            splits,
                                            indexType,
                                            readRowType,
                                            indexField,
                                            userOptions);
                        }

                        // Step 3: commit index meta to a new snapshot
                        commit(table, indexResults);

                        return new InternalRow[] {newInternalRow(true)};
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed to create %s index for column '%s' on table '%s'.",
                                        indexType, column, tableIdent),
                                e);
                    }
                });
    }

    private List<CommitMessage> buildIndex(
            FileStoreTable table,
            Map<BinaryRow, List<IndexedSplit>> preparedDS,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options)
            throws IOException {
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());
        List<Pair<GlobalIndexBuilderContext, byte[]>> taskList = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IndexedSplit>> entry : preparedDS.entrySet()) {
            BinaryRow partition = entry.getKey();
            List<IndexedSplit> partitions = entry.getValue();

            for (IndexedSplit indexedSplit : partitions) {
                checkArgument(
                        indexedSplit.rowRanges().size() == 1,
                        "Each IndexedSplit should contain exactly one row range.");
                GlobalIndexBuilderContext builderContext =
                        new GlobalIndexBuilderContext(
                                table,
                                partition,
                                readType,
                                indexField,
                                indexType,
                                indexedSplit.rowRanges().get(0).from,
                                options);

                byte[] dsBytes = InstantiationUtil.serializeObject(indexedSplit);
                taskList.add(Pair.of(builderContext, dsBytes));
            }
        }

        List<byte[]> commitMessageBytes =
                javaSparkContext
                        .parallelize(taskList)
                        .map(
                                pair -> {
                                    CommitMessageSerializer commitMessageSerializer =
                                            new CommitMessageSerializer();
                                    GlobalIndexBuilderContext builderContext = pair.getLeft();
                                    byte[] dataSplitBytes = pair.getRight();
                                    IndexedSplit split =
                                            InstantiationUtil.deserializeObject(
                                                    dataSplitBytes,
                                                    GlobalIndexBuilder.class.getClassLoader());
                                    GlobalIndexBuilder globalIndexBuilder =
                                            GlobalIndexBuilderFactoryUtils.createIndexBuilder(
                                                    builderContext);
                                    return commitMessageSerializer.serialize(
                                            globalIndexBuilder.build(split));
                                })
                        .collect();

        List<CommitMessage> commitMessages = new ArrayList<>();
        CommitMessageSerializer commitMessageSerializer = new CommitMessageSerializer();
        for (byte[] b : commitMessageBytes) {
            commitMessages.add(
                    commitMessageSerializer.deserialize(commitMessageSerializer.getVersion(), b));
        }
        return commitMessages;
    }

    private void commit(FileStoreTable table, List<CommitMessage> commitMessages) throws Exception {
        try (TableCommitImpl commit = table.newCommit("global-index-create-" + UUID.randomUUID())) {
            commit.commit(commitMessages);
        }
    }

    protected Map<BinaryRow, List<IndexedSplit>> split(
            FileStoreTable table, PartitionPredicate partitions, long rowsPerShard) {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        // Get all manifest entries from the table scan
        List<ManifestEntry> entries =
                table.store().newScan().withPartitionFilter(partitions).plan().files();

        // Group manifest entries by partition
        Map<BinaryRow, List<ManifestEntry>> entriesByPartition =
                entries.stream().collect(Collectors.groupingBy(ManifestEntry::partition));

        return groupFilesIntoShardsByPartition(
                entriesByPartition, rowsPerShard, pathFactory::bucketPath);
    }

    /**
     * Groups files into shards by partition. This method is extracted from split() to make it more
     * testable.
     *
     * @param entriesByPartition manifest entries grouped by partition
     * @param rowsPerShard number of rows per shard
     * @param pathFactory path factory for creating bucket paths
     * @return map of partition to shard splits
     */
    public static Map<BinaryRow, List<IndexedSplit>> groupFilesIntoShardsByPartition(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, Path> pathFactory) {
        Map<BinaryRow, List<IndexedSplit>> result = new HashMap<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partitionEntry :
                entriesByPartition.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            List<ManifestEntry> partitionEntries = partitionEntry.getValue();

            // Group files into shards - a file may belong to multiple shards
            Map<Long, List<DataFileMeta>> filesByShard = new LinkedHashMap<>();

            for (ManifestEntry entry : partitionEntries) {
                DataFileMeta file = entry.file();
                Long firstRowId = file.firstRowId();
                if (firstRowId == null) {
                    continue; // Skip files without row tracking
                }

                // Calculate the row ID range this file covers
                long fileStartRowId = firstRowId;
                long fileEndRowId = firstRowId + file.rowCount() - 1;

                // Calculate which shards this file overlaps with
                long startShardId = fileStartRowId / rowsPerShard;
                long endShardId = fileEndRowId / rowsPerShard;

                // Add this file to all shards it overlaps with
                for (long shardId = startShardId; shardId <= endShardId; shardId++) {
                    long shardStartRowId = shardId * rowsPerShard;
                    filesByShard.computeIfAbsent(shardStartRowId, k -> new ArrayList<>()).add(file);
                }
            }

            // Create DataSplit for each shard with exact ranges
            List<IndexedSplit> shardSplits = new ArrayList<>();
            for (Map.Entry<Long, List<DataFileMeta>> shardEntry : filesByShard.entrySet()) {
                long shardStart = shardEntry.getKey();
                long shardEnd = shardStart + rowsPerShard - 1;
                List<DataFileMeta> shardFiles = shardEntry.getValue();

                if (shardFiles.isEmpty()) {
                    continue;
                }

                // Sort files by firstRowId to ensure sequential order
                shardFiles.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));

                // Group contiguous files and create separate DataSplits for each group
                List<DataFileMeta> currentGroup = new ArrayList<>();
                long currentGroupEnd = -1;

                for (DataFileMeta file : shardFiles) {
                    long fileStart = file.nonNullFirstRowId();
                    long fileEnd = fileStart + file.rowCount() - 1;

                    if (currentGroup.isEmpty()) {
                        // Start a new group
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    } else if (fileStart <= currentGroupEnd + 1) {
                        // File is contiguous with current group (adjacent or overlapping)
                        currentGroup.add(file);
                        currentGroupEnd = Math.max(currentGroupEnd, fileEnd);
                    } else {
                        // Gap detected, finalize current group and start a new one
                        createDataSplitForGroup(
                                currentGroup,
                                shardStart,
                                shardEnd,
                                partition,
                                pathFactory,
                                shardSplits);
                        currentGroup = new ArrayList<>();
                        currentGroup.add(file);
                        currentGroupEnd = fileEnd;
                    }
                }

                // Don't forget to process the last group
                if (!currentGroup.isEmpty()) {
                    createDataSplitForGroup(
                            currentGroup,
                            shardStart,
                            shardEnd,
                            partition,
                            pathFactory,
                            shardSplits);
                }
            }

            if (!shardSplits.isEmpty()) {
                result.put(partition, shardSplits);
            }
        }

        return result;
    }

    private static void createDataSplitForGroup(
            List<DataFileMeta> files,
            long shardStart,
            long shardEnd,
            BinaryRow partition,
            BiFunction<BinaryRow, Integer, Path> pathFactory,
            List<IndexedSplit> shardSplits) {
        // Calculate the actual row range covered by the files
        long groupMinRowId = files.get(0).nonNullFirstRowId();
        long groupMaxRowId =
                files.stream()
                        .mapToLong(f -> f.nonNullFirstRowId() + f.rowCount() - 1)
                        .max()
                        .getAsLong();

        // Clamp to shard boundaries
        // Range.from >= shardStart, Range.to <= shardEnd
        long rangeFrom = Math.max(groupMinRowId, shardStart);
        long rangeTo = Math.min(groupMaxRowId, shardEnd);

        Range range = new Range(rangeFrom, rangeTo);

        DataSplit dataSplit =
                DataSplit.builder()
                        .withPartition(partition)
                        .withBucket(0)
                        .withDataFiles(files)
                        .withBucketPath(pathFactory.apply(partition, 0).toString())
                        .rawConvertible(false)
                        .build();

        shardSplits.add(new IndexedSplit(dataSplit, Collections.singletonList(range), null));
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CreateGlobalIndexProcedure>() {
            @Override
            public CreateGlobalIndexProcedure doBuild() {
                return new CreateGlobalIndexProcedure(tableCatalog());
            }
        };
    }
}
