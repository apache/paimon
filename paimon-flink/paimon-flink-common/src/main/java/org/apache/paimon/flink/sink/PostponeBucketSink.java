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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;

/** {@link FlinkSink} for writing records into fixed bucket Paimon table. */
public class PostponeBucketSink extends FlinkWriteSink<InternalRow> {

    private static final long serialVersionUID = 2L;

    public PostponeBucketSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return createNoStateRowWriteOperatorFactory(table, null, writeProvider, commitUser);
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return createRestoreOnlyCommittableStateManager(table);
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        if (overwritePartition == null) {
            // The table has copied bucket option outside, nothing special for insert into
            return super.createCommitterFactory();
        } else {
            // When overwriting, the postpone bucket files need to be deleted, so using a postpone
            // bucket table commit here
            FileStoreTable tableForCommit =
                    table.copy(
                            Collections.singletonMap(
                                    BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET)));
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
