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

package org.apache.paimon.flink.sink;

import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactTask;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Compaction Sink for BTree global index. */
public class CompactIndexSink extends FlinkSink<BTreeGlobalIndexCompactTask> {

    public CompactIndexSink(FileStoreTable table) {
        super(table, true);
    }

    public static DataStreamSink<?> sink(
            FileStoreTable table, DataStream<BTreeGlobalIndexCompactTask> input) {
        boolean isStreaming = isStreaming(input);
        checkArgument(!isStreaming, "BTree index compaction sink only supports batch mode.");
        return new CompactIndexSink(table).sinkFrom(input);
    }

    @Override
    protected OneInputStreamOperatorFactory<BTreeGlobalIndexCompactTask, Committable>
            createWriteOperatorFactory(StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CompactIndexWorkerOperator.Factory(table, commitUser);
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        return context -> new StoreCommitter(table, table.newCommit(context.commitUser()), context);
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return new NoopCommittableStateManager();
    }
}
