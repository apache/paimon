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

package org.apache.paimon.operation.commit;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ScanMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/** Manifest entries scanner for commit. */
public class CommitScanner {

    private final FileStoreScan scan;
    private final IndexManifestFile indexManifestFile;

    public CommitScanner(
            FileStoreScan scan, IndexManifestFile indexManifestFile, CoreOptions options) {
        this.scan = scan;
        this.indexManifestFile = indexManifestFile;
        // Stats in DELETE Manifest Entries is useless
        if (options.manifestDeleteFileDropStats()) {
            this.scan.dropStats();
        }
    }

    public List<SimpleFileEntry> readIncrementalChanges(
            Snapshot from, Snapshot to, List<BinaryRow> changedPartitions) {
        List<SimpleFileEntry> entries = new ArrayList<>();
        for (long i = from.id() + 1; i <= to.id(); i++) {
            List<SimpleFileEntry> delta =
                    scan.withSnapshot(i)
                            .withKind(ScanMode.DELTA)
                            .withPartitionFilter(changedPartitions)
                            .readSimpleEntries();
            entries.addAll(delta);
        }
        return entries;
    }

    public List<SimpleFileEntry> readAllEntriesFromChangedPartitions(
            Snapshot snapshot, List<BinaryRow> changedPartitions) {
        try {
            return scan.withSnapshot(snapshot)
                    .withKind(ScanMode.ALL)
                    .withPartitionFilter(changedPartitions)
                    .readSimpleEntries();
        } catch (Throwable e) {
            throw new RuntimeException("Cannot read manifest entries from changed partitions.", e);
        }
    }

    public CommitChanges readOverwriteChanges(
            int numBucket,
            List<ManifestEntry> changes,
            List<IndexManifestEntry> indexFiles,
            @Nullable Snapshot latestSnapshot,
            @Nullable PartitionPredicate partitionFilter) {
        List<ManifestEntry> changesWithOverwrite = new ArrayList<>();
        List<IndexManifestEntry> indexChangesWithOverwrite = new ArrayList<>();
        if (latestSnapshot != null) {
            scan.withSnapshot(latestSnapshot)
                    .withPartitionFilter(partitionFilter)
                    .withKind(ScanMode.ALL);
            if (numBucket != BucketMode.POSTPONE_BUCKET) {
                // bucket = -2 can only be overwritten in postpone bucket tables
                scan.withBucketFilter(bucket -> bucket >= 0);
            }
            List<ManifestEntry> currentEntries = scan.plan().files();
            for (ManifestEntry entry : currentEntries) {
                changesWithOverwrite.add(
                        ManifestEntry.create(
                                FileKind.DELETE,
                                entry.partition(),
                                entry.bucket(),
                                entry.totalBuckets(),
                                entry.file()));
            }

            // collect index files
            if (latestSnapshot.indexManifest() != null) {
                List<IndexManifestEntry> entries =
                        indexManifestFile.read(latestSnapshot.indexManifest());
                for (IndexManifestEntry entry : entries) {
                    if (partitionFilter == null || partitionFilter.test(entry.partition())) {
                        indexChangesWithOverwrite.add(entry.toDeleteEntry());
                    }
                }
            }
        }
        changesWithOverwrite.addAll(changes);
        indexChangesWithOverwrite.addAll(indexFiles);
        return new CommitChanges(changesWithOverwrite, emptyList(), indexChangesWithOverwrite);
    }
}
