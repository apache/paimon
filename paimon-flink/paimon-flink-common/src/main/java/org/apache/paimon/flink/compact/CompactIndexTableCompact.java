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

package org.apache.paimon.flink.compact;

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.sink.CompactIndexSink;
import org.apache.paimon.flink.source.CompactIndexSource;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactTask;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;

/**
 * Builder for distributed BTree global index compaction Flink job.
 *
 * <p>Wires a single-parallelism coordinator source that plans compaction tasks, rebalances them
 * across worker operators, and commits the results through a standard Paimon sink.
 */
public class CompactIndexTableCompact {

    private final transient StreamExecutionEnvironment env;
    private final String tableIdentifier;
    private final FileStoreTable table;

    public CompactIndexTableCompact(
            StreamExecutionEnvironment env, String tableIdentifier, FileStoreTable table) {
        this.env = env;
        this.tableIdentifier = tableIdentifier;
        this.table = table;
    }

    public void build() {
        CompactIndexSource source = new CompactIndexSource(table);
        DataStreamSource<BTreeGlobalIndexCompactTask> sourceStream =
                CompactIndexSource.buildSource(env, source, tableIdentifier);

        sinkFromSource(sourceStream);
    }

    private void sinkFromSource(DataStreamSource<BTreeGlobalIndexCompactTask> input) {
        Options conf = Options.fromMap(table.options());
        Integer compactionWorkerParallelism =
                conf.get(FlinkConnectorOptions.UNAWARE_BUCKET_COMPACTION_PARALLELISM);
        PartitionTransformation<BTreeGlobalIndexCompactTask> transformation =
                new PartitionTransformation<>(
                        input.getTransformation(), new RebalancePartitioner<>());
        if (compactionWorkerParallelism != null) {
            transformation.setParallelism(compactionWorkerParallelism);
        } else {
            transformation.setParallelism(env.getParallelism());
        }

        DataStream<BTreeGlobalIndexCompactTask> rebalanced = new DataStream<>(env, transformation);
        CompactIndexSink.sink(table, rebalanced);
    }
}
