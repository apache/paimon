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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.SortCompactSinkBuilder;
import org.apache.paimon.flink.sorter.TableSortInfo;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.sorter.TableSorter.OrderType;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;

/** Compact with sort action. */
public class SortCompactAction extends CompactAction {

    private static final Logger LOG = LoggerFactory.getLogger(SortCompactAction.class);

    private String sortStrategy;
    private List<String> orderColumns;

    public SortCompactAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> catalogConfig,
            Map<String, String> tableConf) {
        super(warehouse, database, tableName, catalogConfig, tableConf);

        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
    }

    @Override
    public void run() throws Exception {
        build();
        execute("Sort Compact Job");
    }

    @Override
    public void build() throws Exception {
        // only support batch sort yet
        if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                != RuntimeExecutionMode.BATCH) {
            LOG.warn(
                    "Sort Compact only support batch mode yet. Please add -Dexecution.runtime-mode=BATCH. The action this time will shift to batch mode forcely.");
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;

        if (fileStoreTable.bucketMode() != BucketMode.BUCKET_UNAWARE
                && fileStoreTable.bucketMode() != BucketMode.HASH_DYNAMIC) {
            throw new IllegalArgumentException("Sort Compact only supports bucket=-1 yet.");
        }
        Map<String, String> tableConfig = fileStoreTable.options();
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(fileStoreTable)
                        .sourceName(
                                ObjectIdentifier.of(
                                                catalogName,
                                                identifier.getDatabaseName(),
                                                identifier.getObjectName())
                                        .asSummaryString());

        sourceBuilder.predicate(getPredicate());

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

        TableSorter sorter = TableSorter.getSorter(env, source, fileStoreTable, sortInfo);

        new SortCompactSinkBuilder(fileStoreTable)
                .forCompact(true)
                .forRowData(sorter.sort())
                .overwrite()
                .build();
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
