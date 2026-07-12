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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.StoreSinkWrite.Provider;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.SequencePreservingPartitionKeyExtractor;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.SerializableFunction;

import java.util.List;

/**
 * A {@link DynamicBucketCompactSink} for the sort compact topology of hash-dynamic bucket tables.
 * It produces a {@link SortCompactCommitter} so that the written append files are committed as a
 * {@code COMPACT} snapshot.
 *
 * <p>The compact-before files (captured by the sort compact plan) are carried as {@link DataSplit}s
 * (which are serializable) into the job graph and used at commit time to build the compact commit
 * messages.
 *
 * <p><b>Scale note:</b> each planned {@link DataSplit} embeds full {@link
 * org.apache.paimon.io.DataFileMeta} objects and is serialized into the committer factory closure.
 * For tables with very large numbers of input files (hundreds of thousands or more), this can
 * significantly inflate the Flink job graph and RPC payload. Consider compacting in smaller
 * partition batches when approaching that scale.
 */
public class SortCompactDynamicBucketSink extends DynamicBucketCompactSink {

    private static final long serialVersionUID = 1L;

    private final long baseSnapshotId;
    private final List<DataSplit> compactInputSplits;
    private final SortCompactPlanMetadata planMetadata;

    public SortCompactDynamicBucketSink(
            FileStoreTable table, long baseSnapshotId, List<DataSplit> compactInputSplits) {
        super(table, null);
        this.baseSnapshotId = baseSnapshotId;
        this.compactInputSplits = compactInputSplits;
        this.planMetadata =
                SortCompactPlanMetadata.capture(table, baseSnapshotId, compactInputSplits);
    }

    @Override
    protected Provider writeProviderOverride() {
        return SortCompactSinkWrite.provider();
    }

    @Override
    protected SerializableFunction<TableSchema, PartitionKeyExtractor<InternalRow>>
            extractorFunction() {
        if (table.coreOptions().snapshotSequenceOrdering() && !table.primaryKeys().isEmpty()) {
            return SequencePreservingPartitionKeyExtractor::new;
        }
        return super.extractorFunction();
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        // Batch sort compact is a one-shot job; use restore-only semantics like append sort
        // compact instead of restore-and-fail, which intentionally fails after recovery commit.
        return FlinkWriteSink.createRestoreOnlyCommittableStateManager(table);
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
