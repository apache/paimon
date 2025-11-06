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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PostponeUtils;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import javax.annotation.Nullable;

import java.util.Map;

/** {@link FlinkSink} for writing records into fixed bucket of postpone table. */
public class PostponeFixedBucketSink extends FlinkWriteSink<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final Map<BinaryRow, Integer> knownNumBuckets;

    public PostponeFixedBucketSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartition,
            Map<BinaryRow, Integer> knownNumBuckets) {
        super(table, overwritePartition);
        this.knownNumBuckets = knownNumBuckets;
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new RowDataStoreWriteOperator.Factory(table, null, writeProvider, commitUser) {
            @Override
            @SuppressWarnings("unchecked, rawtypes")
            public StreamOperator createStreamOperator(StreamOperatorParameters parameters) {
                return new PostponeBatchWriteOperator(
                        parameters, table, writeProvider, commitUser, knownNumBuckets);
            }
        };
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return createRestoreOnlyCommittableStateManager(table);
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        if (overwritePartition == null) {
            // The table has copied bucket option outside, no need to change anything
            return super.createCommitterFactory();
        } else {
            // When overwriting, the postpone bucket files need to be deleted, so using a postpone
            // bucket table commit here
            FileStoreTable tableForCommit = PostponeUtils.tableForCommit(table);
            return context ->
                    new StoreCommitter(
                            tableForCommit,
                            tableForCommit
                                    .newCommit(context.commitUser())
                                    .withOverwrite(overwritePartition)
                                    .ignoreEmptyCommit(!context.streamingCheckpointEnabled()),
                            context);
        }
    }
}
