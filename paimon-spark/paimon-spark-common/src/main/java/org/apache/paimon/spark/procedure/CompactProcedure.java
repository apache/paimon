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
import org.apache.paimon.CoreOptions.OrderType;
import org.apache.paimon.append.AppendCompactCoordinator;
import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.append.cluster.IncrementalClusterManager;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTaskSerializer;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.spark.SparkUtils;
import org.apache.paimon.spark.commands.PaimonSparkWriter;
import org.apache.paimon.spark.sort.TableSorter;
import org.apache.paimon.spark.util.ScanPlanHelper$;
import org.apache.paimon.spark.utils.SparkProcedureUtils;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.AppendCompactTaskSerializer;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ProcedureUtils;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.utils.TimeUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.spark.utils.SparkProcedureUtils.readParallelism;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Compact procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.compact(table => 'tableId', [partitions => 'p1=0,p2=0;p1=0,p2=1'], [order_strategy => 'xxx'], [order_by => 'xxx'], [where => 'p1>0'])
 * </code></pre>
 */
public class CompactProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CompactProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("compact_strategy", StringType),
                ProcedureParameter.optional("order_strategy", StringType),
                ProcedureParameter.optional("order_by", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("options", StringType),
                ProcedureParameter.optional("partition_idle_time", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    private static final String MINOR = "minor";
    private static final String FULL = "full";

    protected CompactProcedure(TableCatalog tableCatalog) {
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
        String compactStrategy = blank(args, 2) ? null : args.getString(2);
        String sortType = blank(args, 3) ? OrderType.NONE.name() : args.getString(3);
        List<String> sortColumns =
                blank(args, 4)
                        ? Collections.emptyList()
                        : Arrays.asList(args.getString(4).split(","));
        String where = blank(args, 5) ? null : args.getString(5);
        String options = args.isNullAt(6) ? null : args.getString(6);
        Duration partitionIdleTime =
                blank(args, 7) ? null : TimeUtils.parseDuration(args.getString(7));
        if (OrderType.NONE.name().equals(sortType) && !sortColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "order_strategy \"none\" cannot work with order_by columns.");
        }
        if (partitionIdleTime != null && (!OrderType.NONE.name().equals(sortType))) {
            throw new IllegalArgumentException(
                    "sort compact do not support 'partition_idle_time'.");
        }

        if (!(compactStrategy == null
                || compactStrategy.equalsIgnoreCase(FULL)
                || compactStrategy.equalsIgnoreCase(MINOR))) {
            throw new IllegalArgumentException(
                    String.format(
                            "The compact strategy only supports 'full' or 'minor', but '%s' is configured.",
                            compactStrategy));
        }

        checkArgument(
                partitions == null || where == null,
                "partitions and where cannot be used together.");
        String finalWhere = partitions != null ? SparkProcedureUtils.toWhere(partitions) : where;
        return modifyPaimonTable(
                tableIdent,
                t -> {
                    checkArgument(t instanceof FileStoreTable);
                    FileStoreTable table = (FileStoreTable) t;
                    CoreOptions coreOptions = table.coreOptions();
                    checkArgument(
                            sortColumns.stream().noneMatch(table.partitionKeys()::contains),
                            "order_by should not contain partition cols, because it is meaningless, your order_by cols are %s, and partition cols are %s",
                            sortColumns,
                            table.partitionKeys());
                    DataSourceV2Relation relation = createRelation(tableIdent);
                    PartitionPredicate partitionPredicate =
                            SparkProcedureUtils.convertToPartitionPredicate(
                                    finalWhere,
                                    table.schema().logicalPartitionType(),
                                    spark(),
                                    relation);
                    HashMap<String, String> dynamicOptions = new HashMap<>();
                    ProcedureUtils.putIfNotEmpty(
                            dynamicOptions, CoreOptions.WRITE_ONLY.key(), "false");
                    ProcedureUtils.putAllOptions(dynamicOptions, options);
                    table = table.copy(dynamicOptions);
                    if (coreOptions.clusteringIncrementalEnabled()
                            && (!OrderType.NONE.name().equals(sortType))) {
                        throw new IllegalArgumentException(
                                "The table has enabled incremental clustering, do not support sort compact.");
                    }

                    InternalRow internalRow =
                            newInternalRow(
                                    execute(
                                            table,
                                            compactStrategy,
                                            sortType,
                                            sortColumns,
                                            relation,
                                            partitionPredicate,
                                            partitionIdleTime));
                    return new InternalRow[] {internalRow};
                });
    }

    @Override
    public String description() {
        return "This procedure execute compact action on paimon table.";
    }

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isNullOrWhitespaceOnly(args.getString(index));
    }

    private boolean execute(
            FileStoreTable table,
            String compactStrategy,
            String sortType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable PartitionPredicate partitionPredicate,
            @Nullable Duration partitionIdleTime) {
        BucketMode bucketMode = table.bucketMode();
        OrderType orderType = OrderType.of(sortType);

        boolean clusterIncrementalEnabled = table.coreOptions().clusteringIncrementalEnabled();
        if (compactStrategy == null) {
            // make full compact strategy as default for compact.
            // make non-full compact strategy as default for incremental clustering.
            compactStrategy = clusterIncrementalEnabled ? MINOR : FULL;
        }
        boolean fullCompact = compactStrategy.equalsIgnoreCase(FULL);
        if (orderType.equals(OrderType.NONE)) {
            JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());
            switch (bucketMode) {
                case HASH_FIXED:
                case HASH_DYNAMIC:
                    compactAwareBucketTable(
                            table,
                            fullCompact,
                            partitionPredicate,
                            partitionIdleTime,
                            javaSparkContext);
                    break;
                case BUCKET_UNAWARE:
                    if (table.coreOptions().dataEvolutionEnabled()) {
                        compactDataEvolutionTable(
                                table, partitionPredicate, partitionIdleTime, javaSparkContext);
                    } else if (clusterIncrementalEnabled) {
                        clusterIncrementalUnAwareBucketTable(
                                table, partitionPredicate, fullCompact, relation);
                    } else {
                        compactUnAwareBucketTable(
                                table, partitionPredicate, partitionIdleTime, javaSparkContext);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Spark compact with " + bucketMode + " is not support yet.");
            }
        } else {
            switch (bucketMode) {
                case BUCKET_UNAWARE:
                    sortCompactUnAwareBucketTable(
                            table, orderType, sortColumns, relation, partitionPredicate);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Spark compact with sort_type "
                                    + sortType
                                    + " only support unaware-bucket append-only table yet.");
            }
        }
        return true;
    }

    private void compactAwareBucketTable(
            FileStoreTable table,
            boolean fullCompact,
            @Nullable PartitionPredicate partitionPredicate,
            @Nullable Duration partitionIdleTime,
            JavaSparkContext javaSparkContext) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Set<BinaryRow> partitionToBeCompacted =
                getHistoryPartition(snapshotReader, partitionIdleTime);
        List<Pair<byte[], Integer>> partitionBuckets =
                snapshotReader.bucketEntries().stream()
                        .map(entry -> Pair.of(entry.partition(), entry.bucket()))
                        .distinct()
                        .filter(pair -> partitionToBeCompacted.contains(pair.getKey()))
                        .map(
                                p ->
                                        Pair.of(
                                                SerializationUtils.serializeBinaryRow(p.getLeft()),
                                                p.getRight()))
                        .collect(Collectors.toList());

        if (partitionBuckets.isEmpty()) {
            LOG.info("Partition bucket is empty, no compact job to execute.");
            return;
        }

        int readParallelism = readParallelism(partitionBuckets, spark());
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        JavaRDD<byte[]> commitMessageJavaRDD =
                javaSparkContext
                        .parallelize(partitionBuckets, readParallelism)
                        .mapPartitions(
                                (FlatMapFunction<Iterator<Pair<byte[], Integer>>, byte[]>)
                                        pairIterator -> {
                                            IOManager ioManager = SparkUtils.createIOManager();
                                            BatchTableWrite write = writeBuilder.newWrite();
                                            write.withIOManager(ioManager);
                                            try {
                                                while (pairIterator.hasNext()) {
                                                    Pair<byte[], Integer> pair =
                                                            pairIterator.next();
                                                    write.compact(
                                                            SerializationUtils.deserializeBinaryRow(
                                                                    pair.getLeft()),
                                                            pair.getRight(),
                                                            fullCompact);
                                                }
                                                CommitMessageSerializer serializer =
                                                        new CommitMessageSerializer();
                                                List<CommitMessage> messages =
                                                        write.prepareCommit();
                                                List<byte[]> serializedMessages =
                                                        new ArrayList<>(messages.size());
                                                for (CommitMessage commitMessage : messages) {
                                                    serializedMessages.add(
                                                            serializer.serialize(commitMessage));
                                                }
                                                return serializedMessages.iterator();
                                            } finally {
                                                write.close();
                                                ioManager.close();
                                            }
                                        });

        try (BatchTableCommit commit = writeBuilder.newCommit()) {
            CommitMessageSerializer serializer = new CommitMessageSerializer();
            List<byte[]> serializedMessages = commitMessageJavaRDD.collect();
            List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
            for (byte[] serializedMessage : serializedMessages) {
                messages.add(serializer.deserialize(serializer.getVersion(), serializedMessage));
            }
            commit.commit(messages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void compactUnAwareBucketTable(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            @Nullable Duration partitionIdleTime,
            JavaSparkContext javaSparkContext) {
        List<AppendCompactTask> compactionTasks;
        try {
            compactionTasks = new AppendCompactCoordinator(table, false, partitionPredicate).run();
        } catch (EndOfScanException e) {
            compactionTasks = new ArrayList<>();
        }
        if (partitionIdleTime != null) {
            SnapshotReader snapshotReader = table.newSnapshotReader();
            if (partitionPredicate != null) {
                snapshotReader.withPartitionFilter(partitionPredicate);
            }
            Map<BinaryRow, Long> partitionInfo =
                    snapshotReader.partitionEntries().stream()
                            .collect(
                                    Collectors.toMap(
                                            PartitionEntry::partition,
                                            PartitionEntry::lastFileCreationTime));
            long historyMilli =
                    LocalDateTime.now()
                            .minus(partitionIdleTime)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
            compactionTasks =
                    compactionTasks.stream()
                            .filter(task -> partitionInfo.get(task.partition()) <= historyMilli)
                            .collect(Collectors.toList());
        }
        if (compactionTasks.isEmpty()) {
            LOG.info("Task plan is empty, no compact job to execute.");
            return;
        }

        AppendCompactTaskSerializer serializer = new AppendCompactTaskSerializer();
        List<byte[]> serializedTasks = new ArrayList<>();
        try {
            for (AppendCompactTask compactionTask : compactionTasks) {
                serializedTasks.add(serializer.serialize(compactionTask));
            }
        } catch (IOException e) {
            throw new RuntimeException("serialize compaction task failed");
        }

        int readParallelism = readParallelism(serializedTasks, spark());
        String commitUser = createCommitUser(table.coreOptions().toConfiguration());
        JavaRDD<byte[]> commitMessageJavaRDD =
                javaSparkContext
                        .parallelize(serializedTasks, readParallelism)
                        .mapPartitions(
                                (FlatMapFunction<Iterator<byte[]>, byte[]>)
                                        taskIterator -> {
                                            BaseAppendFileStoreWrite write =
                                                    (BaseAppendFileStoreWrite)
                                                            table.store().newWrite(commitUser);
                                            CoreOptions coreOptions = table.coreOptions();
                                            if (coreOptions.rowTrackingEnabled()) {
                                                write.withWriteType(
                                                        SpecialFields.rowTypeWithRowTracking(
                                                                table.rowType()));
                                            }
                                            AppendCompactTaskSerializer ser =
                                                    new AppendCompactTaskSerializer();
                                            List<byte[]> messages = new ArrayList<>();
                                            try {
                                                CommitMessageSerializer messageSer =
                                                        new CommitMessageSerializer();
                                                while (taskIterator.hasNext()) {
                                                    AppendCompactTask task =
                                                            ser.deserialize(
                                                                    ser.getVersion(),
                                                                    taskIterator.next());
                                                    messages.add(
                                                            messageSer.serialize(
                                                                    task.doCompact(table, write)));
                                                }
                                                return messages.iterator();
                                            } finally {
                                                write.close();
                                            }
                                        });

        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            CommitMessageSerializer messageSerializerser = new CommitMessageSerializer();
            List<byte[]> serializedMessages = commitMessageJavaRDD.collect();
            List<CommitMessage> messages = new ArrayList<>(serializedMessages.size());
            for (byte[] serializedMessage : serializedMessages) {
                messages.add(
                        messageSerializerser.deserialize(
                                messageSerializerser.getVersion(), serializedMessage));
            }
            commit.commit(messages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void compactDataEvolutionTable(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            @Nullable Duration partitionIdleTime,
            JavaSparkContext javaSparkContext) {
        List<DataEvolutionCompactTask> compactionTasks;
        DataEvolutionCompactCoordinator compactCoordinator =
                new DataEvolutionCompactCoordinator(table, partitionPredicate, false);
        CommitMessageSerializer messageSerializerser = new CommitMessageSerializer();
        String commitUser = createCommitUser(table.coreOptions().toConfiguration());
        try {
            while (true) {
                compactionTasks = compactCoordinator.plan();
                if (partitionIdleTime != null) {
                    SnapshotReader snapshotReader = table.newSnapshotReader();
                    if (partitionPredicate != null) {
                        snapshotReader.withPartitionFilter(partitionPredicate);
                    }
                    Map<BinaryRow, Long> partitionInfo =
                            snapshotReader.partitionEntries().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    PartitionEntry::partition,
                                                    PartitionEntry::lastFileCreationTime));
                    long historyMilli =
                            LocalDateTime.now()
                                    .minus(partitionIdleTime)
                                    .atZone(ZoneId.systemDefault())
                                    .toInstant()
                                    .toEpochMilli();
                    compactionTasks =
                            compactionTasks.stream()
                                    .filter(
                                            task ->
                                                    partitionInfo.get(task.partition())
                                                            <= historyMilli)
                                    .collect(Collectors.toList());
                }
                if (compactionTasks.isEmpty()) {
                    LOG.info("Task plan is empty, no compact job to execute.");
                    continue;
                }

                DataEvolutionCompactTaskSerializer serializer =
                        new DataEvolutionCompactTaskSerializer();
                List<byte[]> serializedTasks = new ArrayList<>();
                try {
                    for (DataEvolutionCompactTask compactionTask : compactionTasks) {
                        serializedTasks.add(serializer.serialize(compactionTask));
                    }
                } catch (IOException e) {
                    throw new RuntimeException("serialize compaction task failed");
                }

                int readParallelism = readParallelism(serializedTasks, spark());
                JavaRDD<byte[]> commitMessageJavaRDD =
                        javaSparkContext
                                .parallelize(serializedTasks, readParallelism)
                                .mapPartitions(
                                        (FlatMapFunction<Iterator<byte[]>, byte[]>)
                                                taskIterator -> {
                                                    DataEvolutionCompactTaskSerializer ser =
                                                            new DataEvolutionCompactTaskSerializer();
                                                    List<byte[]> messagesBytes = new ArrayList<>();
                                                    CommitMessageSerializer messageSer =
                                                            new CommitMessageSerializer();
                                                    while (taskIterator.hasNext()) {
                                                        DataEvolutionCompactTask task =
                                                                ser.deserialize(
                                                                        ser.getVersion(),
                                                                        taskIterator.next());
                                                        messagesBytes.add(
                                                                messageSer.serialize(
                                                                        task.doCompact(
                                                                                table,
                                                                                commitUser)));
                                                    }
                                                    return messagesBytes.iterator();
                                                });

                List<CommitMessage> messages = new ArrayList<>();
                List<byte[]> serializedMessages = commitMessageJavaRDD.collect();
                try (TableCommitImpl commit = table.newCommit(commitUser)) {
                    for (byte[] serializedMessage : serializedMessages) {
                        messages.add(
                                messageSerializerser.deserialize(
                                        messageSerializerser.getVersion(), serializedMessage));
                    }
                    commit.commit(messages);
                } catch (Exception e) {
                    throw new RuntimeException("Deserialize commit message failed", e);
                }
            }
        } catch (EndOfScanException e) {
            LOG.info("Catching EndOfScanException, the compact job is finishing.");
        }
    }

    private Set<BinaryRow> getHistoryPartition(
            SnapshotReader snapshotReader, @Nullable Duration partitionIdleTime) {
        Set<Pair<BinaryRow, Long>> partitionInfo =
                snapshotReader.partitionEntries().stream()
                        .map(
                                partitionEntry ->
                                        Pair.of(
                                                partitionEntry.partition(),
                                                partitionEntry.lastFileCreationTime()))
                        .collect(Collectors.toSet());
        if (partitionIdleTime != null) {
            long historyMilli =
                    LocalDateTime.now()
                            .minus(partitionIdleTime)
                            .atZone(ZoneId.systemDefault())
                            .toInstant()
                            .toEpochMilli();
            partitionInfo =
                    partitionInfo.stream()
                            .filter(partition -> partition.getValue() <= historyMilli)
                            .collect(Collectors.toSet());
        }
        return partitionInfo.stream().map(Pair::getKey).collect(Collectors.toSet());
    }

    private void sortCompactUnAwareBucketTable(
            FileStoreTable table,
            OrderType orderType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable PartitionPredicate partitionPredicate) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader.withPartitionFilter(partitionPredicate);
        }
        Map<BinaryRow, DataSplit[]> packedSplits = packForSort(snapshotReader.read().dataSplits());
        TableSorter sorter = TableSorter.getSorter(table, orderType, sortColumns);
        Dataset<Row> datasetForWrite =
                packedSplits.values().stream()
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

    private void clusterIncrementalUnAwareBucketTable(
            FileStoreTable table,
            @Nullable PartitionPredicate partitionPredicate,
            boolean fullCompaction,
            DataSourceV2Relation relation) {
        IncrementalClusterManager incrementalClusterManager =
                new IncrementalClusterManager(table, partitionPredicate);
        Map<BinaryRow, CompactUnit> compactUnits =
                incrementalClusterManager.createCompactUnits(fullCompaction);

        Map<BinaryRow, Pair<List<DataSplit>, CommitMessage>> partitionSplits =
                incrementalClusterManager.toSplitsAndRewriteDvFiles(compactUnits);

        // sort in partition
        TableSorter sorter =
                TableSorter.getSorter(
                        table,
                        incrementalClusterManager.clusterCurve(),
                        incrementalClusterManager.clusterKeys());
        LOG.info(
                "Start to sort in partition, cluster curve is {}, cluster keys is {}",
                incrementalClusterManager.clusterCurve(),
                incrementalClusterManager.clusterKeys());

        Dataset<Row> datasetForWrite =
                partitionSplits.values().stream()
                        .map(Pair::getKey)
                        .map(
                                splits -> {
                                    Dataset<Row> dataset =
                                            PaimonUtils.createDataset(
                                                    spark(),
                                                    ScanPlanHelper$.MODULE$.createNewScanPlan(
                                                            splits.toArray(new DataSplit[0]),
                                                            relation));
                                    return sorter.sort(dataset);
                                })
                        .reduce(Dataset::union)
                        .orElse(null);
        if (datasetForWrite != null) {
            // set to write only to prevent invoking compaction
            // do not use overwrite, we don't need to overwrite the whole partition
            PaimonSparkWriter writer = PaimonSparkWriter.apply(table).writeOnly();
            Seq<CommitMessage> commitMessages = writer.write(datasetForWrite);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Commit messages after writing:{}", commitMessages);
            }

            // re-organize the commit messages to generate the compact messages
            Map<BinaryRow, List<DataFileMeta>> partitionClustered = new HashMap<>();
            for (CommitMessage commitMessage : JavaConverters.seqAsJavaList(commitMessages)) {
                checkArgument(commitMessage.bucket() == 0);
                partitionClustered
                        .computeIfAbsent(commitMessage.partition(), k -> new ArrayList<>())
                        .addAll(((CommitMessageImpl) commitMessage).newFilesIncrement().newFiles());
            }

            List<CommitMessage> clusterMessages = new ArrayList<>();
            for (Map.Entry<BinaryRow, List<DataFileMeta>> entry : partitionClustered.entrySet()) {
                BinaryRow partition = entry.getKey();
                CommitMessageImpl dvCommitMessage =
                        (CommitMessageImpl) partitionSplits.get(partition).getValue();
                List<DataFileMeta> clusterBefore = compactUnits.get(partition).files();
                // upgrade the clustered file to outputLevel
                List<DataFileMeta> clusterAfter =
                        IncrementalClusterManager.upgrade(
                                entry.getValue(), compactUnits.get(partition).outputLevel());
                LOG.info(
                        "Partition {}: upgrade file level to {}",
                        partition,
                        compactUnits.get(partition).outputLevel());

                List<IndexFileMeta> newIndexFiles = new ArrayList<>();
                List<IndexFileMeta> deletedIndexFiles = new ArrayList<>();
                if (dvCommitMessage != null) {
                    newIndexFiles = dvCommitMessage.compactIncrement().newIndexFiles();
                    deletedIndexFiles = dvCommitMessage.compactIncrement().deletedIndexFiles();
                }

                // get the dv index messages
                CompactIncrement compactIncrement =
                        new CompactIncrement(
                                clusterBefore,
                                clusterAfter,
                                Collections.emptyList(),
                                newIndexFiles,
                                deletedIndexFiles);
                clusterMessages.add(
                        new CommitMessageImpl(
                                partition,
                                // bucket 0 is bucket for unaware-bucket table
                                // for compatibility with the old design
                                0,
                                table.coreOptions().bucket(),
                                DataIncrement.emptyIncrement(),
                                compactIncrement));
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Commit messages after reorganizing:{}", clusterMessages);
            }

            writer.commit(JavaConverters.asScalaBuffer(clusterMessages).toSeq());
        }
    }

    private Map<BinaryRow, DataSplit[]> packForSort(List<DataSplit> dataSplits) {
        // Make a single partition as a compact group
        return dataSplits.stream()
                .collect(
                        Collectors.groupingBy(
                                DataSplit::partition,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        list -> list.toArray(new DataSplit[0]))));
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<CompactProcedure>() {
            @Override
            public CompactProcedure doBuild() {
                return new CompactProcedure(tableCatalog());
            }
        };
    }
}
