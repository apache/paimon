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
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compact with sort action. */
public class SortCompactAction extends CompactAction {

    private String sortStrategy;
    private List<String> orderColumns;

    public SortCompactAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, database, tableName, catalogConfig);

        checkArgument(
                table instanceof AppendOnlyFileStoreTable,
                "Only sort compaction works with append-only table for now.");
        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"));
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // we enable object reuse, we copy the un-reusable object ourselves.
        env.getConfig().enableObjectReuse();

        // kryo serializer is not good
        env.getConfig().disableGenericTypes();

        build(env);
        execute(env, "Sort Compact Job");
    }

    public void build(StreamExecutionEnvironment env) {
        // only support batch sort yet
        if (env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                != RuntimeExecutionMode.BATCH) {
            throw new IllegalArgumentException(
                    "Only support batch mode yet, please set -Dexecution.runtime-mode=BATCH");
        }
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        if (!(fileStoreTable instanceof AppendOnlyFileStoreTable)) {
            throw new IllegalArgumentException("Sort Compact only supports append-only table yet");
        }
        if (fileStoreTable.bucketMode() != BucketMode.UNAWARE) {
            throw new IllegalArgumentException("Sort Compact only supports bucket=-1 yet.");
        }
        Map<String, String> tableConfig = fileStoreTable.options();
        FlinkSourceBuilder sourceBuilder =
                new FlinkSourceBuilder(
                        ObjectIdentifier.of(
                                catalogName,
                                identifier.getDatabaseName(),
                                identifier.getObjectName()),
                        fileStoreTable);

        if (getPartitions() != null) {
            Predicate partitionPredicate =
                    PredicateBuilder.or(
                            getPartitions().stream()
                                    .map(p -> PredicateBuilder.partition(p, table.rowType()))
                                    .toArray(Predicate[]::new));
            sourceBuilder.withPredicate(partitionPredicate);
        }

        String scanParallelism = tableConfig.get(FlinkConnectorOptions.SCAN_PARALLELISM.key());
        if (scanParallelism != null) {
            sourceBuilder.withParallelism(Integer.parseInt(scanParallelism));
        }

        DataStream<RowData> source = sourceBuilder.withEnv(env).withContinuousMode(false).build();
        TableSorter sorter =
                TableSorter.getSorter(env, source, fileStoreTable, sortStrategy, orderColumns);
        DataStream<RowData> sorted = sorter.sort();

        FlinkSinkBuilder flinkSinkBuilder = new FlinkSinkBuilder(fileStoreTable);
        flinkSinkBuilder.withInput(sorted).withOverwritePartition(new HashMap<>());
        String sinkParallelism = tableConfig.get(FlinkConnectorOptions.SINK_PARALLELISM.key());
        if (sinkParallelism != null) {
            flinkSinkBuilder.withParallelism(Integer.parseInt(sinkParallelism));
        }

        flinkSinkBuilder.build();
    }

    public void withOrderStrategy(String sortStrategy) {
        this.sortStrategy = sortStrategy;
    }

    public void withOrderColumns(List<String> orderColumns) {
        this.orderColumns = orderColumns;
    }
}
