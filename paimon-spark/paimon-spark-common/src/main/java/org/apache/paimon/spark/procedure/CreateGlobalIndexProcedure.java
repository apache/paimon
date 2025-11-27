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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilder;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderContext;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderFactory;
import org.apache.paimon.spark.globalindex.GlobalIndexBuilderFactoryUtils;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.StringUtils;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

        // Early validation: check if the index type is supported
        GlobalIndexBuilderFactory globalIndexBuilderFactory =
                GlobalIndexBuilderFactoryUtils.load(indexType);

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
                        Map<BinaryRow, Map<Range, DataSplit>> splits =
                                split(table, partitionPredicate, rowsPerShard);

                        // Step 2: build index by certain index system
                        List<IndexManifestEntry> indexResults =
                                buildIndex(
                                        table,
                                        splits,
                                        indexType,
                                        readRowType,
                                        indexField,
                                        userOptions,
                                        globalIndexBuilderFactory);

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

    private List<IndexManifestEntry> buildIndex(
            FileStoreTable table,
            Map<BinaryRow, Map<Range, DataSplit>> preparedDS,
            String indexType,
            RowType readType,
            DataField indexField,
            Options options,
            GlobalIndexBuilderFactory globalIndexBuilderFactory) {
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<List<IndexManifestEntry>>> futures = new ArrayList<>();
        try {
            for (Map.Entry<BinaryRow, Map<Range, DataSplit>> entry : preparedDS.entrySet()) {
                BinaryRow partition = entry.getKey();
                Map<Range, DataSplit> partitions = entry.getValue();

                for (Map.Entry<Range, DataSplit> partitionEntry : partitions.entrySet()) {
                    Range startOffset = partitionEntry.getKey();
                    DataSplit partitionDS = partitionEntry.getValue();
                    GlobalIndexBuilderContext builderContext =
                            new GlobalIndexBuilderContext(
                                    spark(),
                                    table,
                                    partition,
                                    readType,
                                    indexField,
                                    indexType,
                                    startOffset,
                                    options);

                    futures.add(
                            executor.submit(
                                    () -> {
                                        GlobalIndexBuilder globalIndexBuilder =
                                                globalIndexBuilderFactory.create(builderContext);
                                        return globalIndexBuilder.build(partitionDS);
                                    }));
                }
            }

            List<IndexManifestEntry> entries = new ArrayList<>();
            for (Future<List<IndexManifestEntry>> future : futures) {
                try {
                    entries.addAll(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Index creation was interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("Build index failed", e.getCause());
                }
            }

            return entries;
        } finally {
            executor.shutdown();
        }
    }

    private void commit(FileStoreTable table, List<IndexManifestEntry> indexResults)
            throws Exception {
        Map<BinaryRow, List<IndexFileMeta>> partitionResults =
                indexResults.stream()
                        .map(s -> Pair.of(s.partition(), s.indexFile()))
                        .collect(
                                Collectors.groupingBy(
                                        Pair::getKey,
                                        Collectors.mapping(Pair::getValue, Collectors.toList())));

        List<CommitMessage> commitMessages = new ArrayList<>();
        for (Map.Entry<BinaryRow, List<IndexFileMeta>> entry : partitionResults.entrySet()) {
            BinaryRow partition = entry.getKey();
            List<IndexFileMeta> indexFiles = entry.getValue();
            commitMessages.add(
                    new CommitMessageImpl(
                            partition,
                            0,
                            null,
                            DataIncrement.indexIncrement(indexFiles),
                            CompactIncrement.emptyIncrement()));
        }

        try (TableCommitImpl commit = table.newCommit("global-index-create-" + UUID.randomUUID())) {
            commit.commit(commitMessages);
        }
    }

    protected Map<BinaryRow, Map<Range, DataSplit>> split(
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
    public static Map<BinaryRow, Map<Range, DataSplit>> groupFilesIntoShardsByPartition(
            Map<BinaryRow, List<ManifestEntry>> entriesByPartition,
            long rowsPerShard,
            BiFunction<BinaryRow, Integer, Path> pathFactory) {
        Map<BinaryRow, Map<Range, DataSplit>> result = new HashMap<>();

        for (Map.Entry<BinaryRow, List<ManifestEntry>> partitionEntry :
                entriesByPartition.entrySet()) {
            BinaryRow partition = partitionEntry.getKey();
            List<ManifestEntry> partitionEntries = partitionEntry.getValue();

            // Group files into shards - a file may belong to multiple shards
            Map<Long, List<DataFileMeta>> filesByShard = new HashMap<>();

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
            Map<Range, DataSplit> shardSplits = new HashMap<>();

            for (Map.Entry<Long, List<DataFileMeta>> shardEntry : filesByShard.entrySet()) {
                long startRowId = shardEntry.getKey();
                List<DataFileMeta> shardFiles = shardEntry.getValue();

                if (shardFiles.isEmpty()) {
                    continue;
                }

                // Use exact shard boundaries: [n*rowsPerShard, (n+1)*rowsPerShard - 1]
                long minRowId = startRowId;
                long maxRowId = startRowId + rowsPerShard - 1;
                Range range = new Range(minRowId, maxRowId);

                // Create DataSplit for this shard
                DataSplit dataSplit =
                        DataSplit.builder()
                                .withPartition(partition)
                                .withBucket(0)
                                .withDataFiles(shardFiles)
                                .withBucketPath(pathFactory.apply(partition, 0).toString())
                                .rawConvertible(false)
                                .build();

                shardSplits.put(range, dataSplit);
            }

            if (!shardSplits.isEmpty()) {
                result.put(partition, shardSplits);
            }
        }

        return result;
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
