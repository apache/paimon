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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestCommittableSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.FlinkConnectorOptions.PARTITION_MARK_DONE_RECOVER_FROM_STATE;

/** A {@link FlinkSink} to write records. */
public abstract class FlinkWriteSink<T> extends FlinkSink<T> {

    private static final long serialVersionUID = 1L;

    @Nullable protected final Map<String, String> overwritePartition;

    public FlinkWriteSink(FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition != null);
        this.overwritePartition = overwritePartition;
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        // If checkpoint is enabled for streaming job, we have to
        // commit new files list even if they're empty.
        // Otherwise we can't tell if the commit is successful after
        // a restart.
        return context ->
                new StoreCommitter(
                        table,
                        table.newCommit(context.commitUser())
                                .withOverwrite(overwritePartition)
                                .ignoreEmptyCommit(!context.streamingCheckpointEnabled()),
                        context);
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        Options options = table.coreOptions().toConfiguration();
        return new RestoreAndFailCommittableStateManager<>(
                ManifestCommittableSerializer::new,
                options.get(PARTITION_MARK_DONE_RECOVER_FROM_STATE));
    }

    protected static OneInputStreamOperatorFactory<InternalRow, Committable>
            createNoStateRowWriteOperatorFactory(
                    FileStoreTable table,
                    StoreSinkWrite.Provider writeProvider,
                    String commitUser) {
        return new RowDataStoreWriteOperator.Factory(table, writeProvider, commitUser) {
            @Override
            @SuppressWarnings("unchecked, rawtypes")
            public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
                return new StatelessRowDataStoreWriteOperator(
                        parameters, table, writeProvider, commitUser);
            }
        };
    }

    protected static CommittableStateManager<ManifestCommittable>
            createRestoreOnlyCommittableStateManager(FileStoreTable table) {
        Options options = table.coreOptions().toConfiguration();
        return new RestoreCommittableStateManager<>(
                ManifestCommittableSerializer::new,
                options.get(PARTITION_MARK_DONE_RECOVER_FROM_STATE));
    }
}
