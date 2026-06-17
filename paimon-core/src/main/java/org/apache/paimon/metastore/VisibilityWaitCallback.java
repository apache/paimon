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
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.utils.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** A {@link CommitCallback} to wait for compaction and global indexes for visibility. */
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
        List<Range> rowIdRangesToTrack = collectRowIdRangesToTrack(context.deltaFiles);
        Set<GlobalIndexIdentifier> globalIndexesToTrack =
                rowIdRangesToTrack.isEmpty()
                        ? Collections.emptySet()
                        : collectGlobalIndexesToTrack(context.snapshot);
        for (ManifestEntry entry : context.deltaFiles) {
            if (shouldBeTracked(entry)) {
                namesToTrack.add(entry.fileName());
                partitionsToTrack.add(entry.partition());
            }
        }

        if (namesToTrack.isEmpty()
                && (rowIdRangesToTrack.isEmpty() || globalIndexesToTrack.isEmpty())) {
            return;
        }

        try {
            waitForVisibility(
                    context.snapshot,
                    namesToTrack,
                    partitionsToTrack,
                    rowIdRangesToTrack,
                    globalIndexesToTrack);
        } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void retry(ManifestCommittable committable) {
        // No-op for retry as the callback is idempotent
    }

    private void waitForVisibility(
            Snapshot fromSnapshot,
            Set<String> namesToTrack,
            Set<BinaryRow> partitionsToTrack,
            List<Range> rowIdRangesToTrack,
            Set<GlobalIndexIdentifier> globalIndexesToTrack)
            throws InterruptedException, TimeoutException {
        long startTime = System.currentTimeMillis();
        boolean compactionDone = namesToTrack.isEmpty();
        boolean globalIndexDone = rowIdRangesToTrack.isEmpty() || globalIndexesToTrack.isEmpty();
        Snapshot checkedSnapshot = fromSnapshot;
        while (System.currentTimeMillis() - startTime < timeout.toMillis()) {
            Snapshot latest = table.snapshotManager().latestSnapshot();
            checkNotNull(latest, "No latest snapshot");

            if (!compactionDone
                    && latest.id() > checkedSnapshot.id()
                    && !stillInLatest(latest, namesToTrack, partitionsToTrack)) {
                compactionDone = true;
            }
            if (!globalIndexDone
                    && globalIndexesBuilt(latest, rowIdRangesToTrack, globalIndexesToTrack)) {
                globalIndexDone = true;
            }
            if (compactionDone && globalIndexDone) {
                return;
            }
            checkedSnapshot = latest;

            LOG.info(
                    "Waiting for visibility of table {}. Compaction done: {}, global index done: {}.",
                    table.fullName(),
                    compactionDone,
                    globalIndexDone);
            //noinspection BusyWait
            Thread.sleep(checkInterval.toMillis());
        }

        throw new TimeoutException(
                "Timeout waiting for files to be compacted or global indexes to be built after "
                        + timeout);
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

    private List<Range> collectRowIdRangesToTrack(List<ManifestEntry> deltaFiles) {
        List<Range> ranges = new ArrayList<>();
        for (ManifestEntry entry : deltaFiles) {
            if (shouldTrackGlobalIndex(entry)) {
                ranges.add(entry.file().nonNullRowIdRange());
            }
        }
        return Range.sortAndMergeOverlap(ranges, true);
    }

    private boolean shouldTrackGlobalIndex(ManifestEntry entry) {
        if (!FileKind.ADD.equals(entry.kind())) {
            return false;
        }

        DataFileMeta file = entry.file();
        if (file.firstRowId() == null || file.rowCount() <= 0) {
            return false;
        }

        Optional<FileSource> fileSource = file.fileSource();
        if (!fileSource.isPresent() || !FileSource.APPEND.equals(fileSource.get())) {
            return false;
        }

        String fileName = file.fileName();
        return !isBlobFile(fileName) && !isVectorStoreFile(fileName);
    }

    private Set<GlobalIndexIdentifier> collectGlobalIndexesToTrack(Snapshot snapshot) {
        Set<GlobalIndexIdentifier> indexes = new HashSet<>();
        for (IndexManifestEntry entry : scanGlobalIndexes(snapshot)) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex != null) {
                indexes.add(
                        new GlobalIndexIdentifier(
                                entry.indexFile().indexType(),
                                globalIndex.indexFieldId(),
                                globalIndex.extraFieldIds()));
            }
        }
        return indexes;
    }

    private boolean globalIndexesBuilt(
            Snapshot snapshot,
            List<Range> rowIdRangesToTrack,
            Set<GlobalIndexIdentifier> globalIndexesToTrack) {
        Map<GlobalIndexIdentifier, List<Range>> indexedRanges = new HashMap<>();
        for (IndexManifestEntry entry : scanGlobalIndexes(snapshot)) {
            GlobalIndexMeta globalIndex = entry.indexFile().globalIndexMeta();
            if (globalIndex == null) {
                continue;
            }

            GlobalIndexIdentifier identifier =
                    new GlobalIndexIdentifier(
                            entry.indexFile().indexType(),
                            globalIndex.indexFieldId(),
                            globalIndex.extraFieldIds());
            if (globalIndexesToTrack.contains(identifier)) {
                indexedRanges
                        .computeIfAbsent(identifier, k -> new ArrayList<>())
                        .add(globalIndex.rowRange());
            }
        }

        for (GlobalIndexIdentifier identifier : globalIndexesToTrack) {
            List<Range> ranges = Range.sortAndMergeOverlap(indexedRanges.get(identifier), true);
            for (Range rowIdRange : rowIdRangesToTrack) {
                if (!rowIdRange.exclude(ranges).isEmpty()) {
                    return false;
                }
            }
        }

        return true;
    }

    private List<IndexManifestEntry> scanGlobalIndexes(Snapshot snapshot) {
        return table.store().newIndexFileHandler().scan(snapshot, this::isGlobalIndex);
    }

    private boolean isGlobalIndex(IndexManifestEntry entry) {
        return entry.indexFile().globalIndexMeta() != null;
    }

    @Override
    public void close() throws Exception {
        // No resources to close
    }

    private static class GlobalIndexIdentifier {

        private final String indexType;
        private final int indexFieldId;
        private final int[] extraFieldIds;

        private GlobalIndexIdentifier(String indexType, int indexFieldId, int[] extraFieldIds) {
            this.indexType = indexType;
            this.indexFieldId = indexFieldId;
            this.extraFieldIds =
                    extraFieldIds == null
                            ? null
                            : Arrays.copyOf(extraFieldIds, extraFieldIds.length);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof GlobalIndexIdentifier)) {
                return false;
            }

            GlobalIndexIdentifier that = (GlobalIndexIdentifier) o;
            return indexFieldId == that.indexFieldId
                    && Objects.equals(indexType, that.indexType)
                    && Arrays.equals(extraFieldIds, that.extraFieldIds);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(indexType, indexFieldId);
            result = 31 * result + Arrays.hashCode(extraFieldIds);
            return result;
        }
    }
}
