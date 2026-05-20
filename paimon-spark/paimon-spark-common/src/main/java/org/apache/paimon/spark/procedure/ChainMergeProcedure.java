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

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.SparkUtils;
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionUtils;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Option;

import static org.apache.paimon.spark.utils.SparkProcedureUtils.toWhere;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Chain merge procedure. Usage:
 *
 * <pre><code>
 *  CALL sys.compact(table => 'tableId', partitions => 'p1=0,p2=0', target_branch => 'snapshot')
 * </code></pre>
 */
public class ChainMergeProcedure extends BaseProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(ChainMergeProcedure.class);

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.required("partitions", StringType),
                ProcedureParameter.optional("target_branch", StringType),
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField("result", DataTypes.BooleanType, true, Metadata.empty())
                    });

    protected ChainMergeProcedure(TableCatalog tableCatalog) {
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

    private boolean blank(InternalRow args, int index) {
        return args.isNullAt(index) || StringUtils.isNullOrWhitespaceOnly(args.getString(index));
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        String partitions = args.getString(1);
        SparkTable sparkTable = loadSparkTable(tableIdent);
        String targetBranch =
                blank(args, 2)
                        ? sparkTable.coreOptions().scanFallbackSnapshotBranch()
                        : args.getString(2);
        List<Map<String, String>> compactPartitions =
                ParameterUtils.getPartitions(partitions.split(";"));
        validataChainMerge(sparkTable, targetBranch, partitions, compactPartitions);
        DataSourceV2Relation relation = createRelation(tableIdent);
        Expression condition =
                getPartitionCondition(relation, sparkTable.table(), toWhere(partitions));
        boolean executed =
                executeChainMerge(
                        (FallbackReadFileStoreTable) sparkTable.getTable(),
                        condition,
                        relation,
                        targetBranch);
        return new InternalRow[] {newInternalRow(executed)};
    }

    public boolean executeChainMerge(
            FallbackReadFileStoreTable chainTable,
            Expression partCondition,
            DataSourceV2Relation relation,
            String targetBranch) {

        // build scan for the specific partition
        Preconditions.checkArgument(
                chainTable.other() instanceof ChainGroupReadTable,
                "The chain merge should perform on the ChainFileStoreTable");

        Option<Predicate> filter =
                ExpressionUtils.convertConditionToPaimonPredicate(
                        partCondition,
                        ((LogicalPlan) relation).output(),
                        chainTable.rowType(),
                        false);

        ChainGroupReadTable chainGroupReadTable = (ChainGroupReadTable) chainTable.other();
        ChainGroupReadTable.ChainTableBatchScan scan =
                (ChainGroupReadTable.ChainTableBatchScan) chainGroupReadTable.newScan();
        if (filter.isDefined()) {
            scan.withFilter(filter.get());
        }
        List<Split> splits = scan.plan().splits();

        if (splits.isEmpty()) {
            LOG.info("The target partition={} is empty", partCondition);
            return false;
        }
        Preconditions.checkArgument(
                splits.stream().allMatch(s -> (s instanceof ChainSplit)),
                "The chain merge only accepts ChainDataSplit");

        // build snapshot branch write builder with static partition overwrite
        FileStoreTable targetTable = ((ChainGroupReadTable) chainTable.other()).wrapped();
        checkArgument(
                targetBranch.equals(targetTable.coreOptions().branch()),
                "chain_merge should merge to snapshot branch");
        InternalRowPartitionComputer computer =
                new InternalRowPartitionComputer(
                        chainTable.coreOptions().partitionDefaultName(),
                        chainTable.schema().logicalPartitionType(),
                        chainTable.schema().partitionKeys().toArray(new String[0]),
                        chainTable.coreOptions().legacyPartitionName());
        Map<String, String> targetPartition =
                computer.generatePartValues(((ChainSplit) splits.get(0)).logicalPartition());

        LOG.info(
                "Direct chain_merge plan built, splits: {}, target partition: {}, target branch: {}",
                splits.size(),
                targetPartition,
                targetBranch);
        BatchWriteBuilder writeBuilder =
                targetTable.newBatchWriteBuilder().withOverwrite(targetPartition);
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark().sparkContext());

        JavaRDD<byte[]> commitMessageJavaRDD =
                javaSparkContext
                        .parallelize(splits)
                        .mapPartitions(
                                (FlatMapFunction<Iterator<Split>, byte[]>)
                                        splitIterator -> {
                                            List<byte[]> serializedMessages = new ArrayList<>();
                                            IOManager ioManager = SparkUtils.createIOManager();
                                            BatchTableWrite write = writeBuilder.newWrite();
                                            write.withIOManager(ioManager);
                                            while (splitIterator.hasNext()) {
                                                Split split = splitIterator.next();
                                                try {
                                                    TableRead read =
                                                            chainGroupReadTable
                                                                    .newRead()
                                                                    .withIOManager(ioManager);
                                                    RecordReader<org.apache.paimon.data.InternalRow>
                                                            reader = read.createReader(split);
                                                    try (RecordReader<
                                                                    org.apache.paimon.data
                                                                            .InternalRow>
                                                            rr = reader) {
                                                        RecordReaderIterator<
                                                                        org.apache.paimon.data
                                                                                .InternalRow>
                                                                it = new RecordReaderIterator<>(rr);
                                                        org.apache.paimon.data.InternalRow row;
                                                        while ((row = it.next()) != null) {
                                                            write.write(row);
                                                        }
                                                    }
                                                    CommitMessageSerializer serializer =
                                                            new CommitMessageSerializer();
                                                    List<CommitMessage> messages =
                                                            write.prepareCommit();
                                                    for (CommitMessage commitMessage : messages) {
                                                        serializedMessages.add(
                                                                serializer.serialize(
                                                                        commitMessage));
                                                    }
                                                } finally {
                                                    write.close();
                                                    ioManager.close();
                                                }
                                            }
                                            return serializedMessages.iterator();
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
        return true;
    }

    private Expression getPartitionCondition(
            DataSourceV2Relation relation, Table table, String where) {
        Expression condition = null;
        if (!StringUtils.isNullOrWhitespaceOnly(where)) {
            condition = ExpressionUtils.resolveFilter(spark(), relation, where);
            checkArgument(
                    ExpressionUtils.isValidPredicate(
                            spark(), condition, table.partitionKeys().toArray(new String[0])),
                    "Only partition predicate is supported, your predicate is %s, but partition keys are %s",
                    condition,
                    table.partitionKeys());
        }
        return condition;
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ChainMergeProcedure>() {
            @Override
            public ChainMergeProcedure doBuild() {
                return new ChainMergeProcedure(tableCatalog());
            }
        };
    }

    private void validataChainMerge(
            SparkTable sparkTable,
            String targetBranch,
            String partitions,
            List<Map<String, String>> compactPartitions) {
        checkArgument(
                sparkTable.coreOptions().isChainTable(), "chain_merge only supports chain table");
        checkArgument(
                targetBranch.equals(sparkTable.coreOptions().scanFallbackSnapshotBranch()),
                "chain_merge should merge to snapshot branch");
        checkArgument(
                sparkTable.getTable() instanceof FallbackReadFileStoreTable,
                "The chain merge should perform on the chain table");
        checkArgument(
                compactPartitions.size() == 1
                        && compactPartitions.get(0).size()
                                == sparkTable.table().partitionKeys().size(),
                "chain_merge only supports one partition %s",
                partitions);
    }
}
