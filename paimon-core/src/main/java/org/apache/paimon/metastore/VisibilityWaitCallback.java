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

package org.apache.paimon.metastore;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link CommitCallback} to wait for compaction for visibility. */
public class VisibilityWaitCallback implements CommitCallback {

    private static final Logger LOG = LoggerFactory.getLogger(VisibilityWaitCallback.class);

    private final FileStoreTable table;
    private final boolean deletionVectorsEnabled;
    private final Duration timeout;
    private final Duration checkInterval;

    public VisibilityWaitCallback(FileStoreTable table) {
        this.table = table;
        CoreOptions options = table.coreOptions();
        this.deletionVectorsEnabled = options.deletionVectorsEnabled();
        this.timeout = options.visibilityCallbackTimeout();
        this.checkInterval = options.visibilityCallbackCheckInterval();
    }

    @Override
    public void call(Context context) {
        // only work for batch or bounded stream
        if (context.identifier != BatchWriteBuilder.COMMIT_IDENTIFIER) {
            return;
        }

        Set<String> namesToTrack = new HashSet<>();
        Set<BinaryRow> partitionsToTrack = new HashSet<>();
        for (ManifestEntry entry : context.deltaFiles) {
            if (shouldBeTracked(entry)) {
                namesToTrack.add(entry.fileName());
                partitionsToTrack.add(entry.partition());
            }
        }

        if (namesToTrack.isEmpty()) {
            return;
        }

        try {
            waitForCompaction(context.snapshot, namesToTrack, partitionsToTrack);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void retry(ManifestCommittable committable) {
        // No-op for retry as the callback is idempotent
    }

    private void waitForCompaction(
            Snapshot fromSnapshot, Set<String> namesToTrack, Set<BinaryRow> partitionsToTrack)
            throws InterruptedException, TimeoutException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
            Snapshot latest = table.snapshotManager().latestSnapshot();
            checkNotNull(latest, "No latest snapshot");
            if (latest.id() > fromSnapshot.id()
                    && !stillInLatest(latest, namesToTrack, partitionsToTrack)) {
                return;
            }
            fromSnapshot = latest;

            LOG.info("Waiting for files of table {} to be compacted...", table.fullName());
            //noinspection BusyWait
            Thread.sleep(checkInterval.toMillis());
        }

        throw new TimeoutException("Timeout waiting for files to be compacted after " + timeout);
    }

    private boolean stillInLatest(
            Snapshot snapshot, Set<String> namesToTrack, Set<BinaryRow> partitionsToTrack) {
        Iterator<ManifestEntry> iterator =
                table.newSnapshotReader()
                        .withSnapshot(snapshot)
                        .withPartitionFilter(new ArrayList<>(partitionsToTrack))
                        .readFileIterator();
        while (iterator.hasNext()) {
            ManifestEntry entry = iterator.next();
            if (shouldBeTracked(entry) && namesToTrack.contains(entry.file().fileName())) {
                return true;
            }
        }

        return false;
    }

    private boolean shouldBeTracked(ManifestEntry entry) {
        if (!FileKind.ADD.equals(entry.kind())) {
            return false;
        }

        if (deletionVectorsEnabled && entry.level() == 0) {
            return true;
        }

        return entry.bucket() == BucketMode.POSTPONE_BUCKET;
    }

    @Override
    public void close() throws Exception {
        // No resources to close
    }
}
