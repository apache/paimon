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

import org.apache.paimon.append.SortCompactCommitMessageRewriter;
import org.apache.paimon.append.SortCompactPlanMetadata;
import org.apache.paimon.flink.sink.StoreSinkWrite.Provider;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;

import javax.annotation.Nullable;

import java.util.List;

/**
 * A {@link RowAppendTableSink} for the sort compact topology of bucket-unaware append tables. It
 * produces a {@link SortCompactCommitter} so that the written append files are committed as a
 * {@code COMPACT} snapshot.
 *
 * <p>The compact-before files (captured by the sort compact plan) are carried as {@link DataSplit}s
 * (which are serializable) into the job graph and used at commit time to build the compact commit
 * messages.
 *
 * <p><b>Scale note:</b> see {@link SortCompactSinkBuilder#withSortCompactInput(long, List)}.
 */
public class SortCompactAppendTableSink extends RowAppendTableSink {

    private static final long serialVersionUID = 1L;

    private final long baseSnapshotId;
    private final List<DataSplit> compactInputSplits;
    private final SortCompactPlanMetadata planMetadata;

    public SortCompactAppendTableSink(
            FileStoreTable table,
            @Nullable Integer parallelism,
            long baseSnapshotId,
            List<DataSplit> compactInputSplits) {
        super(table, null, parallelism);
        this.baseSnapshotId = baseSnapshotId;
        this.compactInputSplits = compactInputSplits;
        this.planMetadata =
                SortCompactPlanMetadata.capture(table, baseSnapshotId, compactInputSplits);
    }

    @Override
    protected Provider writeProviderOverride() {
        return SortCompactAppendSinkWrite.provider();
    }

    @Override
    protected Committer.Factory<Committable, ManifestCommittable> createCommitterFactory() {
        // If checkpoint is enabled for streaming job, we have to commit new files list even if
        // they're empty. Otherwise we can't tell if the commit is successful after a restart.
        return context ->
                new SortCompactCommitter(
                        table,
                        table.newCommit(context.commitUser())
                                .ignoreEmptyCommit(!context.streamingCheckpointEnabled()),
                        context,
                        new SortCompactCommitMessageRewriter(
                                table, baseSnapshotId, compactInputSplits, planMetadata));
    }
}
