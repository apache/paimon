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
import org.apache.paimon.CoreOptions.OrderType;
import org.apache.paimon.append.SortCompactSequenceUtils;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.SortCompactSinkBuilder;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Compact with sort action. */
public class SortCompactAction extends CompactAction {

    private static final Logger LOG = LoggerFactory.getLogger(SortCompactAction.class);

    private String sortStrategy;
    private List<String> orderColumns;

    public SortCompactAction(
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(database, tableName, catalogConfig, tableConf);
        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
    }

    @Override
    public void run() throws Exception {
        if (buildImpl()) {
            execute("Sort Compact Job");
        }
    }

    @Override
    protected boolean buildImpl() throws Exception {
        // only support batch sort yet
        if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                != RuntimeExecutionMode.BATCH) {
            LOG.warn(
                    "Sort Compact only support batch mode yet. Please add -Dexecution.runtime-mode=BATCH. The action this time will shift to batch mode forcely.");
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        if (fileStoreTable.coreOptions().dataEvolutionEnabled()) {
            throw new UnsupportedOperationException("Data Evolution table cannot be sorted!");
        }

        if (fileStoreTable.coreOptions().rowTrackingEnabled()) {
            throw new UnsupportedOperationException(
                    "Sort compact is unsupported for row tracking tables.");
        }

        if (fileStoreTable.bucketMode() != BucketMode.BUCKET_UNAWARE
                && fileStoreTable.bucketMode() != BucketMode.HASH_DYNAMIC) {
            throw new IllegalArgumentException("Sort Compact only supports bucket=-1 yet.");
        }

        // Capture the base snapshot and the planned input splits. The old files in these splits
        // become compactBefore of the compact commit, so that sort compact is committed as a
        // normal COMPACT commit (instead of OVERWRITE) and does not drop data appended
        // concurrently since the base snapshot.
        PartitionPredicate partitionPredicate = getPartitionPredicate();
        SnapshotReader snapshotReader = fileStoreTable.newSnapshotReader();
        if (partitionPredicate != null) {
            snapshotReader.withPartitionFilter(partitionPredicate);
        }
        SnapshotReader.Plan plan = snapshotReader.read();
        Long baseSnapshotId = plan.snapshotId();
        List<DataSplit> dataSplits = plan.dataSplits();
        if (dataSplits.isEmpty()) {
            // empty table or no matching partitions: no-op job with no commit
            return false;
        }

        // Pin the source to the captured base snapshot so that the sort compact reads exactly
        // the planned data, while the sink stays write-only.
        Map<String, String> sourceOptions = new HashMap<>();
        sourceOptions.put(
                CoreOptions.SCAN_SNAPSHOT_ID.key(),
                String.valueOf(baseSnapshotId == null ? 0L : baseSnapshotId));
        RowType sortRowType = fileStoreTable.rowType();
        if (fileStoreTable.coreOptions().snapshotSequenceOrdering()
                && !fileStoreTable.primaryKeys().isEmpty()) {
            sourceOptions.put(CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(), "true");
            sortRowType = SortCompactSequenceUtils.rowTypeWithKeyValueSequenceNumber(sortRowType);
        }
        FileStoreTable sourceTable = fileStoreTable.copy(sourceOptions);

        Map<String, String> tableConfig = fileStoreTable.options();
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(sourceTable)
                        .sourceName(
                                ObjectIdentifier.of(
                                                catalogName,
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .asSummaryString())
                        .readType(sortRowType);

        sourceBuilder.partitionPredicate(partitionPredicate);

        String scanParallelism = tableConfig.get(FlinkConnectorOptions.SCAN_PARALLELISM.key());
        if (scanParallelism != null) {
            sourceBuilder.sourceParallelism(Integer.parseInt(scanParallelism));
        }

        DataStream<RowData> source = sourceBuilder.env(env).sourceBounded(true).build();
        int localSampleMagnification =
                ((FileStoreTable) table).coreOptions().getLocalSampleMagnification();
        if (localSampleMagnification < 20) {
            throw new IllegalArgumentException(
                    String.format(
                            "the config '%s=%d' should not be set too small,greater than or equal to 20 is needed.",
                            CoreOptions.SORT_COMPACTION_SAMPLE_MAGNIFICATION.key(),
                            localSampleMagnification));
        }
        String sinkParallelismValue =
                table.options().get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        final int sinkParallelism =
                sinkParallelismValue == null
                        ? source.getParallelism()
                        : Integer.parseInt(sinkParallelismValue);
        TableSortInfo sortInfo =
                new TableSortInfo.Builder()
                        .setSortColumns(orderColumns)
                        .setSortStrategy(OrderType.of(sortStrategy))
                        .setSinkParallelism(sinkParallelism)
                        .setLocalSampleSize(sinkParallelism * localSampleMagnification)
                        .setGlobalSampleSize(sinkParallelism * 1000)
                        .setRangeNumber(sinkParallelism * 10)
                        .build();

        TableSorter sorter =
                TableSorter.getSorter(
                        env, source, fileStoreTable.coreOptions(), sortRowType, sortInfo);

        new SortCompactSinkBuilder(fileStoreTable)
                .forCompact(true)
                .withSortCompactInput(baseSnapshotId == null ? 0L : baseSnapshotId, dataSplits)
                .forRowData(sorter.sort())
                .build();
        return true;
    }

    public SortCompactAction withOrderStrategy(String sortStrategy) {
        this.sortStrategy = sortStrategy;
        return this;
    }

    public SortCompactAction withOrderColumns(String... orderColumns) {
        return withOrderColumns(Arrays.asList(orderColumns));
    }

    public SortCompactAction withOrderColumns(List<String> orderColumns) {
        this.orderColumns = orderColumns.stream().map(String::trim).collect(Collectors.toList());
        return this;
    }
}
