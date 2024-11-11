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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Statistics for a commit. */
public class CommitStats {
    private final long duration;
    private final int attempts;
    private final long tableFilesAdded;
    private final long tableFilesAppended;
    private final long tableFilesDeleted;
    private final long changelogFilesAppended;
    private final long compactionInputFileSize;
    private final long compactionOutputFileSize;
    private final long changelogFilesCompacted;
    private final long changelogRecordsCompacted;

    private final long deltaRecordsCompacted;
    private final long changelogRecordsAppended;
    private final long deltaRecordsAppended;
    private final long tableFilesCompacted;
    private final long generatedSnapshots;
    private final long numPartitionsWritten;
    private final long numBucketsWritten;

    public CommitStats(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> appendChangelogFiles,
            List<ManifestEntry> compactTableFiles,
            List<ManifestEntry> compactChangelogFiles,
            long commitDuration,
            int generatedSnapshots,
            int attempts) {
        List<ManifestEntry> addedTableFiles = new ArrayList<>(appendTableFiles);
        List<ManifestEntry> compactAfterFiles =
                compactTableFiles.stream()
                        .filter(f -> FileKind.ADD.equals(f.kind()))
                        .collect(Collectors.toList());
        addedTableFiles.addAll(compactAfterFiles);
        List<ManifestEntry> deletedTableFiles =
                compactTableFiles.stream()
                        .filter(f -> FileKind.DELETE.equals(f.kind()))
                        .collect(Collectors.toList());

        this.compactionInputFileSize =
                deletedTableFiles.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::fileSize)
                        .reduce(Long::sum)
                        .orElse(0L);
        this.compactionOutputFileSize =
                compactAfterFiles.stream()
                        .map(ManifestEntry::file)
                        .map(DataFileMeta::fileSize)
                        .reduce(Long::sum)
                        .orElse(0L);
        this.tableFilesAdded = addedTableFiles.size();
        this.tableFilesAppended = appendTableFiles.size();
        this.tableFilesDeleted = deletedTableFiles.size();
        this.tableFilesCompacted = compactTableFiles.size();
        this.changelogFilesAppended = appendChangelogFiles.size();
        this.changelogFilesCompacted = compactChangelogFiles.size();
        this.numPartitionsWritten = numChangedPartitions(appendTableFiles, compactTableFiles);
        this.numBucketsWritten = numChangedBuckets(appendTableFiles, compactTableFiles);
        this.changelogRecordsCompacted = getRowCounts(compactChangelogFiles);
        this.deltaRecordsCompacted = getRowCounts(compactTableFiles);
        this.changelogRecordsAppended = getRowCounts(appendChangelogFiles);
        this.deltaRecordsAppended = getRowCounts(appendTableFiles);
        this.duration = commitDuration;
        this.generatedSnapshots = generatedSnapshots;
        this.attempts = attempts;
    }

    @VisibleForTesting
    protected static long numChangedPartitions(List<ManifestEntry>... changes) {
        return Arrays.stream(changes)
                .flatMap(Collection::stream)
                .map(ManifestEntry::partition)
                .distinct()
                .count();
    }

    @VisibleForTesting
    protected static long numChangedBuckets(List<ManifestEntry>... changes) {
        return changedPartBuckets(changes).values().stream().mapToLong(Set::size).sum();
    }

    @VisibleForTesting
    protected static List<BinaryRow> changedPartitions(List<ManifestEntry>... changes) {
        return Arrays.stream(changes)
                .flatMap(Collection::stream)
                .map(ManifestEntry::partition)
                .distinct()
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    protected static Map<BinaryRow, Set<Integer>> changedPartBuckets(
            List<ManifestEntry>... changes) {
        Map<BinaryRow, Set<Integer>> changedPartBuckets = new LinkedHashMap<>();
        Arrays.stream(changes)
                .flatMap(Collection::stream)
                .forEach(
                        entry ->
                                changedPartBuckets
                                        .computeIfAbsent(
                                                entry.partition(), k -> new LinkedHashSet<>())
                                        .add(entry.bucket()));
        return changedPartBuckets;
    }

    private long getRowCounts(List<ManifestEntry> files) {
        return files.stream().mapToLong(file -> file.file().rowCount()).sum();
    }

    @VisibleForTesting
    protected long getTableFilesAdded() {
        return tableFilesAdded;
    }

    @VisibleForTesting
    protected long getTableFilesDeleted() {
        return tableFilesDeleted;
    }

    @VisibleForTesting
    protected long getTableFilesAppended() {
        return tableFilesAppended;
    }

    @VisibleForTesting
    protected long getTableFilesCompacted() {
        return tableFilesCompacted;
    }

    @VisibleForTesting
    protected long getChangelogFilesAppended() {
        return changelogFilesAppended;
    }

    @VisibleForTesting
    protected long getChangelogFilesCompacted() {
        return changelogFilesCompacted;
    }

    @VisibleForTesting
    protected long getGeneratedSnapshots() {
        return generatedSnapshots;
    }

    @VisibleForTesting
    protected long getDeltaRecordsAppended() {
        return deltaRecordsAppended;
    }

    @VisibleForTesting
    protected long getChangelogRecordsAppended() {
        return changelogRecordsAppended;
    }

    @VisibleForTesting
    protected long getDeltaRecordsCompacted() {
        return deltaRecordsCompacted;
    }

    @VisibleForTesting
    protected long getChangelogRecordsCompacted() {
        return changelogRecordsCompacted;
    }

    @VisibleForTesting
    protected long getNumPartitionsWritten() {
        return numPartitionsWritten;
    }

    @VisibleForTesting
    protected long getNumBucketsWritten() {
        return numBucketsWritten;
    }

    @VisibleForTesting
    protected long getDuration() {
        return duration;
    }

    @VisibleForTesting
    protected int getAttempts() {
        return attempts;
    }

    public long getCompactionInputFileSize() {
        return compactionInputFileSize;
    }

    public long getCompactionOutputFileSize() {
        return compactionOutputFileSize;
    }
}
