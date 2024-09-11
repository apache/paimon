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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareAppendTableCompactionCoordinator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.spark.PaimonSplitScan;
import org.apache.paimon.spark.SparkUtils;
import org.apache.paimon.spark.catalyst.Compatibility;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.spark.commands.PaimonSparkWriter;
import org.apache.paimon.spark.sort.TableSorter;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.sink.CompactionTaskSerializer;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ParameterUtils;
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
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.createCommitUser;
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

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType),
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
        String sortType = blank(args, 2) ? TableSorter.OrderType.NONE.name() : args.getString(2);
        List<String> sortColumns =
                blank(args, 3)
                        ? Collections.emptyList()
                        : Arrays.asList(args.getString(3).split(","));
        String where = blank(args, 4) ? null : args.getString(4);
        String options = args.isNullAt(5) ? null : args.getString(5);
        Duration partitionIdleTime =
                blank(args, 6) ? null : TimeUtils.parseDuration(args.getString(6));
        if (TableSorter.OrderType.NONE.name().equals(sortType) && !sortColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "order_strategy \"none\" cannot work with order_by columns.");
        }
        if (partitionIdleTime != null && (!TableSorter.OrderType.NONE.name().equals(sortType))) {
            throw new IllegalArgumentException(
                    "sort compact do not support 'partition_idle_time'.");
        }
        checkArgument(
                partitions == null || where == null,
                "partitions and where cannot be used together.");
        String finalWhere = partitions != null ? toWhere(partitions) : where;
        return modifyPaimonTable(
                tableIdent,
                table -> {
                    checkArgument(table instanceof FileStoreTable);
                    checkArgument(
                            sortColumns.stream().noneMatch(table.partitionKeys()::contains),
                            "order_by should not contain partition cols, because it is meaningless, your order_by cols are %s, and partition cols are %s",
                            sortColumns,
                            table.partitionKeys());
                    DataSourceV2Relation relation = createRelation(tableIdent);
                    Expression condition = null;
                    if (!StringUtils.isNullOrWhitespaceOnly(finalWhere)) {
                        condition = ExpressionUtils.resolveFilter(spark(), relation, finalWhere);
                        checkArgument(
                                ExpressionUtils.isValidPredicate(
                                        spark(),
                                        condition,
                                        table.partitionKeys().toArray(new String[0])),
                                "Only partition predicate is supported, your predicate is %s, but partition keys are %s",
                                condition,
                                table.partitionKeys());
                    }

                    Map<String, String> dynamicOptions = new HashMap<>();
                    dynamicOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
                    if (!StringUtils.isNullOrWhitespaceOnly(options)) {
                        dynamicOptions.putAll(ParameterUtils.parseCommaSeparatedKeyValues(options));
                    }
                    table = table.copy(dynamicOptions);

                    InternalRow internalRow =
                            newInternalRow(
                                    execute(
                                            (FileStoreTable) table,
                                            sortType,
                                            sortColumns,
                                            relation,
                                            condition,
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
            String sortType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable Expression condition,
            @Nullable Duration partitionIdleTime) {
        BucketMode bucketMode = table.bucketMode();
        TableSorter.OrderType orderType = TableSorter.OrderType.of(sortType);
        Predicate filter =
                condition == null
                        ? null
                        : ExpressionUtils.convertConditionToPaimonPredicate(
                                        condition,
                                        ((LogicalPlan) relation).output(),
                                        table.rowType(),
                                        false)
                                .getOrElse(null);
        if (orderType.equals(TableSorter.OrderType.NONE)) {
            JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());
            switch (bucketMode) {
                case HASH_FIXED:
                case HASH_DYNAMIC:
                    compactAwareBucketTable(table, filter, partitionIdleTime, javaSparkContext);
                    break;
                case BUCKET_UNAWARE:
                    compactUnAwareBucketTable(table, filter, partitionIdleTime, javaSparkContext);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Spark compact with " + bucketMode + " is not support yet.");
            }
        } else {
            switch (bucketMode) {
                case BUCKET_UNAWARE:
                    sortCompactUnAwareBucketTable(table, orderType, sortColumns, relation, filter);
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
            @Nullable Predicate filter,
            @Nullable Duration partitionIdleTime,
            JavaSparkContext javaSparkContext) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (filter != null) {
            snapshotReader.withFilter(filter);
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
            return;
        }

        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        JavaRDD<byte[]> commitMessageJavaRDD =
                javaSparkContext
                        .parallelize(partitionBuckets)
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
                                                            true);
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
            @Nullable Predicate filter,
            @Nullable Duration partitionIdleTime,
            JavaSparkContext javaSparkContext) {
        List<UnawareAppendCompactionTask> compactionTasks =
                new UnawareAppendTableCompactionCoordinator(table, false, filter).run();
        if (partitionIdleTime != null) {
            Map<BinaryRow, Long> partitionInfo =
                    table.newSnapshotReader().partitionEntries().stream()
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
            return;
        }

        CompactionTaskSerializer serializer = new CompactionTaskSerializer();
        List<byte[]> serializedTasks = new ArrayList<>();
        try {
            for (UnawareAppendCompactionTask compactionTask : compactionTasks) {
                serializedTasks.add(serializer.serialize(compactionTask));
            }
        } catch (IOException e) {
            throw new RuntimeException("serialize compaction task failed");
        }

        String commitUser = createCommitUser(table.coreOptions().toConfiguration());
        JavaRDD<byte[]> commitMessageJavaRDD =
                javaSparkContext
                        .parallelize(serializedTasks)
                        .mapPartitions(
                                (FlatMapFunction<Iterator<byte[]>, byte[]>)
                                        taskIterator -> {
                                            AppendOnlyFileStoreWrite write =
                                                    (AppendOnlyFileStoreWrite)
                                                            table.store().newWrite(commitUser);
                                            CompactionTaskSerializer ser =
                                                    new CompactionTaskSerializer();
                                            List<byte[]> messages = new ArrayList<>();
                                            try {
                                                CommitMessageSerializer messageSer =
                                                        new CommitMessageSerializer();
                                                while (taskIterator.hasNext()) {
                                                    UnawareAppendCompactionTask task =
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
            TableSorter.OrderType orderType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable Predicate filter) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (filter != null) {
            snapshotReader.withFilter(filter);
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
                                                    Compatibility.createDataSourceV2ScanRelation(
                                                            relation,
                                                            PaimonSplitScan.apply(table, split),
                                                            relation.output()));
                                    return sorter.sort(dataset);
                                })
                        .reduce(Dataset::union)
                        .orElse(null);
        if (datasetForWrite != null) {
            PaimonSparkWriter writer = new PaimonSparkWriter(table);
            // Use dynamic partition overwrite
            writer.writeBuilder().withOverwrite();
            writer.commit(writer.write(datasetForWrite));
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

    @VisibleForTesting
    static String toWhere(String partitions) {
        List<Map<String, String>> maps = ParameterUtils.getPartitions(partitions.split(";"));

        return maps.stream()
                .map(
                        a ->
                                a.entrySet().stream()
                                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                                        .reduce((s0, s1) -> s0 + " AND " + s1))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(a -> "(" + a + ")")
                .reduce((a, b) -> a + " OR " + b)
                .orElse(null);
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
