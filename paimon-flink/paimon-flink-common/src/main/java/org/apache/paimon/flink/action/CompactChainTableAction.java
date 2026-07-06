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

package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.ChainGroupReadTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ParameterUtils;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.SCAN_DEDICATED_SPLIT_GENERATION;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Action to compact chain table by merging snapshot and delta branches into the snapshot branch.
 */
public class CompactChainTableAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(CompactChainTableAction.class);

    public static final String IDENTIFIER = "compact_chain_table";

    protected String partition;

    protected boolean overwrite;

    public CompactChainTableAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String partition,
            Boolean overwrite) {
        super(databaseName, tableName, catalogConfig);
        this.partition = partition;
        this.overwrite = overwrite != null && overwrite;
    }

    public CompactChainTableAction withOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public CompactChainTableAction withPartition(String partition) {
        this.partition = partition;
        return this;
    }

    @Override
    public void build() throws Exception {
        checkArgument(
                StringUtils.isNotEmpty(partition),
                "Partition string cannot be empty for compact_chain_table");
        checkArgument(
                !partition.contains(";"),
                "compact_chain_table only supports a single partition, but multiple partitions were provided: %s",
                partition);
        checkArgument(
                new CoreOptions(table.options()).isChainTable(),
                "compact_chain_table only supports chain table");
        checkArgument(
                table instanceof FallbackReadFileStoreTable,
                "Table %s is not a chain table",
                identifier.getFullName());
        checkArgument(
                env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.BATCH,
                "compact_chain_table only supports batch execution mode");
        checkArgument(
                !Options.fromMap(table.options()).get(SCAN_DEDICATED_SPLIT_GENERATION),
                "compact_chain_table does not support %s.",
                SCAN_DEDICATED_SPLIT_GENERATION.key());

        ChainGroupReadTable chainTable =
                (ChainGroupReadTable) ((FallbackReadFileStoreTable) table).other();
        FileStoreTable snapshotTable = chainTable.wrapped();

        RowType partitionType = snapshotTable.schema().logicalPartitionType();
        String partitionDefaultName = snapshotTable.coreOptions().partitionDefaultName();
        Map<String, String> partitionSpec = ParameterUtils.parseCommaSeparatedKeyValues(partition);
        Predicate partitionPredicate =
                PredicateBuilder.partition(partitionSpec, partitionType, partitionDefaultName);
        checkArgument(
                partitionPredicate != null,
                "Failed to build partition predicate for partition: %s",
                partition);
        PartitionPredicate predicate =
                PartitionPredicate.fromPredicate(partitionType, partitionPredicate);

        boolean partitionExists =
                !snapshotTable.newScan().withPartitionFilter(predicate).plan().splits().isEmpty();

        if (partitionExists && !overwrite) {
            LOG.info(
                    "Partition {} already exists in snapshot branch, skipping compaction.",
                    partition);
            buildEmptyPipeline();
            return;
        }
        DataStream<RowData> source =
                new FlinkSourceBuilder(chainTable)
                        .env(env)
                        .sourceBounded(true)
                        .partitionPredicate(predicate)
                        .withSkipPreloadTargetSnapshot(partitionExists && overwrite)
                        .sourceName(identifier.getFullName() + "-chain-compact-source")
                        .build();

        FlinkSinkBuilder sinkBuilder = new FlinkSinkBuilder(snapshotTable).forRowData(source);
        if (partitionExists) {
            sinkBuilder.overwrite(partitionSpec);
        }
        sinkBuilder.build();
    }

    private void buildEmptyPipeline() {
        env.fromSequence(0, 0).sinkTo(new DiscardingSink<>());
    }
}
