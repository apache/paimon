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

package org.apache.paimon.flink.source;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.flink.source.assigners.DynamicPartitionPruningAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.table.connector.source.DynamicFilteringEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link SplitEnumerator} implementation for {@link StaticFileStoreSource} input. */
public class StaticFileStoreSplitEnumerator
        implements SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(StaticFileStoreSplitEnumerator.class);

    private final SplitEnumeratorContext<FileStoreSourceSplit> context;

    @Nullable private final Snapshot snapshot;

    private SplitAssigner splitAssigner;

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner) {
        this(context, snapshot, splitAssigner, null);
    }

    public StaticFileStoreSplitEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            @Nullable Snapshot snapshot,
            SplitAssigner splitAssigner,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        this.context = context;
        this.snapshot = snapshot;
        this.splitAssigner = splitAssigner;
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo;
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!context.registeredReaders().containsKey(subtask)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        List<FileStoreSourceSplit> assignment = splitAssigner.getNext(subtask, hostname);
        if (assignment.size() > 0) {
            context.assignSplits(
                    new SplitsAssignment<>(Collections.singletonMap(subtask, assignment)));
        } else {
            context.signalNoMoreSplits(subtask);
        }
    }

    @Override
    public void addSplitsBack(List<FileStoreSourceSplit> backSplits, int subtaskId) {
        splitAssigner.addSplitsBack(subtaskId, backSplits);
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) {
        return new PendingSplitsCheckpoint(
                splitAssigner.remainingSplits(), snapshot == null ? null : snapshot.id());
    }

    @Override
    public void close() {
        // no resources to close
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof ReaderConsumeProgressEvent) {
            // batch reading doesn't handle consumer
            // avoid meaningless error logs
            return;
        }

        if (sourceEvent.getClass().getSimpleName().equals("DynamicFilteringEvent")) {
            checkNotNull(
                    dynamicPartitionFilteringInfo,
                    "Cannot apply dynamic filtering because dynamicPartitionFilteringInfo hasn't been set.");

            if (splitAssigner instanceof PreAssignSplitAssigner) {
                this.splitAssigner =
                        ((PreAssignSplitAssigner) splitAssigner)
                                .ofDynamicPartitionPruning(
                                        dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                                        ((DynamicFilteringEvent) sourceEvent).getData());
            } else {
                this.splitAssigner =
                        DynamicPartitionPruningAssigner
                                .createDynamicPartitionPruningAssignerIfNeeded(
                                        subtaskId,
                                        splitAssigner,
                                        dynamicPartitionFilteringInfo.getPartitionRowProjection(),
                                        sourceEvent,
                                        LOG);
            }
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    @VisibleForTesting
    public SplitAssigner getSplitAssigner() {
        return splitAssigner;
    }
}
