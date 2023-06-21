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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * It is only used to notify {@link AlignedSourceReader} of the checkpointId that is about to be
 * triggered.
 */
public class AlignedEnumerator implements SplitEnumerator<AlignedSourceSplit, Void> {

    private final SplitEnumeratorContext<AlignedSourceSplit> context;

    public AlignedEnumerator(SplitEnumeratorContext<AlignedSourceSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
        // do nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // ignore
    }

    @Override
    public void addSplitsBack(List<AlignedSourceSplit> splits, int subtaskId) {
        // ignore
    }

    @Override
    public void addReader(int subtaskId) {
        // ignore
    }

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        Preconditions.checkArgument(
                context.currentParallelism() == 1,
                "The parallelism of the aligned source must be 1.");
        context.sendEventToSourceReader(0, new CheckpointEvent(checkpointId));
        return null;
    }

    @Override
    public void close() throws IOException {
        // ignore
    }
}
