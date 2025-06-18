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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.flink.sink.Committer;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Partition listeners. */
public class CommitListeners implements Closeable {

    private final List<CommitListener> listeners;

    private CommitListeners(List<CommitListener> listeners) {
        this.listeners = listeners;
    }

    public void notifyCommittable(List<ManifestCommittable> committables) {
        for (CommitListener listener : listeners) {
            listener.notifyCommittable(committables);
        }
    }

    public void notifyCommittable(
            List<ManifestCommittable> committables, boolean partitionMarkDoneRecoverFromState) {
        for (CommitListener listener : listeners) {
            if (partitionMarkDoneRecoverFromState
                    || !(listener instanceof PartitionMarkDoneListener)) {
                listener.notifyCommittable(committables);
            }
        }
    }

    public void snapshotState() throws Exception {
        for (CommitListener listener : listeners) {
            listener.snapshotState();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAllQuietly(listeners);
    }

    public static CommitListeners create(Committer.Context context, FileStoreTable table)
            throws Exception {
        List<CommitListener> listeners = new ArrayList<>();

        // partition statistics reporter
        ReportPartStatsListener.create(context.isRestored(), context.stateStore(), table)
                .ifPresent(listeners::add);

        // partition mark done
        PartitionMarkDoneListener.create(
                        context.getClass().getClassLoader(),
                        context.streamingCheckpointEnabled(),
                        context.isRestored(),
                        context.stateStore(),
                        table)
                .ifPresent(listeners::add);

        return new CommitListeners(listeners);
    }
}
