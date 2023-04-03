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

import org.apache.paimon.flink.VersionedSerializerWrapper;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableStateManager;
import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.flink.sink.LogSinkFunction;
import org.apache.paimon.flink.sink.RestoreAndFailCommittableStateManager;
import org.apache.paimon.flink.sink.StoreCommitter;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.util.function.SerializableFunction;

import javax.annotation.Nullable;

/**
 * A {@link FlinkSink} which accepts {@link CdcRecord} and waits for a schema change if necessary.
 */
public class FlinkCdcSink extends FlinkSink<CdcRecord> {

    private static final long serialVersionUID = 1L;

    private final Lock.Factory lockFactory;
    @Nullable private final LogSinkFunction logSinkFunction;

    public FlinkCdcSink(
            FileStoreTable table,
            Lock.Factory lockFactory,
            @Nullable LogSinkFunction logSinkFunction) {
        super(table, false);
        this.lockFactory = lockFactory;
        this.logSinkFunction = logSinkFunction;
    }

    @Override
    protected OneInputStreamOperator<CdcRecord, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, boolean isStreaming) {
        return new SchemaAwareStoreWriteOperator(table, logSinkFunction, writeProvider);
    }

    @Override
    protected SerializableFunction<String, Committer> createCommitterFactory(
            boolean streamingCheckpointEnabled) {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return user ->
                new StoreCommitter(
                        table.newCommit(user)
                                .withLock(lockFactory.create())
                                .ignoreEmptyCommit(!streamingCheckpointEnabled));
    }

    @Override
    protected CommittableStateManager createCommittableStateManager() {
        return new RestoreAndFailCommittableStateManager(
                () -> new VersionedSerializerWrapper<>(new ManifestCommittableSerializer()));
    }
}
