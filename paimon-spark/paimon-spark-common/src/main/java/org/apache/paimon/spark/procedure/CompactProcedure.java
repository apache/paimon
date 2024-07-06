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
import org.apache.paimon.utils.ExecutorThreadFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.SerializationUtils;
import org.apache.paimon.utils.StringUtils;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;

import static org.apache.paimon.CoreOptions.createCommitUser;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Compact procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.compact(table => 'tableId', [partitions => 'p1=0,p2=0;p1=0,p2=1'], [order_strategy => 'xxx'], [order_by => 'xxx'], [where => 'p1>0'])
 * </code></pre>
 */
public class CompactProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(CompactProcedure.class.getName());

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("partitions", StringType),
                ProcedureParameter.optional("order_strategy", StringType),
                ProcedureParameter.optional("order_by", StringType),
                ProcedureParameter.optional("where", StringType),
                ProcedureParameter.optional("max_concurrent_jobs", IntegerType),
                ProcedureParameter.optional("options", StringType),
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
        int maxConcurrentJobs = args.isNullAt(5) ? 15 : args.getInt(5);
        String options = args.isNullAt(6) ? null : args.getString(6);
        if (TableSorter.OrderType.NONE.name().equals(sortType) && !sortColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "order_strategy \"none\" cannot work with order_by columns.");
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
                    if (!StringUtils.isBlank(finalWhere)) {
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
                    if (!StringUtils.isBlank(options)) {
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
                                            maxConcurrentJobs));
                    return new InternalRow[] {internalRow};
                });
    }

    @Override
    public String description() {
        return "This procedure execute compact action on paimon table.";
    }

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isBlank(args.getString(index));
    }

    private boolean execute(
            FileStoreTable table,
            String sortType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable Expression condition,
            int maxConcurrentJobs) {
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
                    compactAwareBucketTable(table, filter, javaSparkContext);
                    break;
                case BUCKET_UNAWARE:
                    compactUnAwareBucketTable(table, filter, javaSparkContext);
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Spark compact with " + bucketMode + " is not support yet.");
            }
        } else {
            switch (bucketMode) {
                case BUCKET_UNAWARE:
                    sortCompactUnAwareBucketTable(
                            table, orderType, sortColumns, relation, filter, maxConcurrentJobs);
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
            FileStoreTable table, @Nullable Predicate filter, JavaSparkContext javaSparkContext) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (filter != null) {
            snapshotReader.withFilter(filter);
        }

        List<Pair<byte[], Integer>> partitionBuckets =
                snapshotReader.read().splits().stream()
                        .map(split -> (DataSplit) split)
                        .map(dataSplit -> Pair.of(dataSplit.partition(), dataSplit.bucket()))
                        .distinct()
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
            FileStoreTable table, @Nullable Predicate filter, JavaSparkContext javaSparkContext) {
        List<UnawareAppendCompactionTask> compactionTasks =
                new UnawareAppendTableCompactionCoordinator(table, false, filter).run();
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

    private void sortCompactUnAwareBucketTable(
            FileStoreTable table,
            TableSorter.OrderType orderType,
            List<String> sortColumns,
            DataSourceV2Relation relation,
            @Nullable Predicate filter,
            int maxConcurrentJobs) {
        SnapshotReader snapshotReader = table.newSnapshotReader();
        if (filter != null) {
            snapshotReader.withFilter(filter);
        }
        Map<BinaryRow, DataSplit[]> packedSplits = packForSort(snapshotReader.read().dataSplits());

        PaimonSparkWriter writer = new PaimonSparkWriter(table);
        // Use dynamic partition overwrite
        writer.writeBuilder().withOverwrite();

        ExecutorService executorService =
                Executors.newFixedThreadPool(
                        Math.min(maxConcurrentJobs, packedSplits.size()),
                        new ExecutorThreadFactory(Thread.currentThread().getName() + "-compact"));
        LinkedList<Future<Seq<CommitMessage>>> futures = new LinkedList<>();
        TableSorter sorter = TableSorter.getSorter(table, orderType, sortColumns);

        LOG.info("Start submit sort compact jobs, count: {}", packedSplits.size());
        for (Map.Entry<BinaryRow, DataSplit[]> entry : packedSplits.entrySet()) {
            Dataset<Row> dataset =
                    PaimonUtils.createDataset(
                            spark(),
                            Compatibility.createDataSourceV2ScanRelation(
                                    relation,
                                    PaimonSplitScan.apply(table, entry.getValue()),
                                    relation.output()));
            futures.add(executorService.submit(() -> writer.write(sorter.sort(dataset))));
        }

        ListBuffer<CommitMessage> messages = new ListBuffer<>();
        try {
            for (Future<Seq<CommitMessage>> future : futures) {
                messages.append(future.get());
            }
        } catch (Throwable e) {
            throw new RuntimeException("Compact failed", e);
        } finally {
            executorService.shutdownNow();
        }

        writer.commit(messages.toSeq());
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
