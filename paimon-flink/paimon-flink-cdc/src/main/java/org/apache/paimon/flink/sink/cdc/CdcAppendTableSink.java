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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.FlinkWriteSink;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

/**
 * CDC Sink for unaware bucket table. It should not add compaction node, because the compaction may
 * have old schema.
 */
public class CdcAppendTableSink extends FlinkWriteSink<CdcRecord> {

    private final Integer parallelism;
    private final boolean noShuffle;

    public CdcAppendTableSink(FileStoreTable table, Integer parallelism) {
        this(table, parallelism, false);
    }

    public CdcAppendTableSink(FileStoreTable table, Integer parallelism, boolean noShuffle) {
        super(table, null);
        this.parallelism = parallelism;
        this.noShuffle = noShuffle;
    }

    @Override
    protected OneInputStreamOperatorFactory<CdcRecord, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new CdcAppendTableWriteOperator.Factory(table, writeProvider, commitUser);
    }

    @Override
    public DataStream<Committable> doWrite(
            DataStream<CdcRecord> input, String initialCommitUser, @Nullable Integer parallelism) {
        DataStream<Committable> written = super.doWrite(input, initialCommitUser, this.parallelism);
        if (noShuffle) {
            // Break operator chaining between parse and write to avoid deadlock
            // during schema evolution retries, without introducing a network shuffle.
            ((SingleOutputStreamOperator<Committable>) written).startNewChain();
        }
        return written;
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return createRestoreOnlyCommittableStateManager(table);
    }
}
