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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** Detailed changes from {@link CommitMessage}s. */
public class ManifestEntryChanges {

    private final int defaultNumBucket;

    public List<ManifestEntry> appendTableFiles;
    public List<ManifestEntry> appendChangelog;
    public List<IndexManifestEntry> appendIndexFiles;
    public List<ManifestEntry> compactTableFiles;
    public List<ManifestEntry> compactChangelog;
    public List<IndexManifestEntry> compactIndexFiles;

    public ManifestEntryChanges(int defaultNumBucket) {
        this.defaultNumBucket = defaultNumBucket;
        this.appendTableFiles = new ArrayList<>();
        this.appendChangelog = new ArrayList<>();
        this.appendIndexFiles = new ArrayList<>();
        this.compactTableFiles = new ArrayList<>();
        this.compactChangelog = new ArrayList<>();
        this.compactIndexFiles = new ArrayList<>();
    }

    public void collect(CommitMessage message) {
        CommitMessageImpl commitMessage = (CommitMessageImpl) message;
        commitMessage
                .newFilesIncrement()
                .newFiles()
                .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .deletedFiles()
                .forEach(m -> appendTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .changelogFiles()
                .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .deletedIndexFiles()
                .forEach(
                        m ->
                                appendIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.DELETE,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
        commitMessage
                .newFilesIncrement()
                .newIndexFiles()
                .forEach(
                        m ->
                                appendIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));

        commitMessage
                .compactIncrement()
                .compactBefore()
                .forEach(m -> compactTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .compactAfter()
                .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .changelogFiles()
                .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .deletedIndexFiles()
                .forEach(
                        m ->
                                compactIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.DELETE,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
        commitMessage
                .compactIncrement()
                .newIndexFiles()
                .forEach(
                        m ->
                                compactIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
    }

    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        Integer totalBuckets = commitMessage.totalBuckets();
        if (totalBuckets == null) {
            totalBuckets = defaultNumBucket;
        }

        return ManifestEntry.create(
                kind, commitMessage.partition(), commitMessage.bucket(), totalBuckets, file);
    }

    @Override
    public String toString() {
        List<String> msg = new ArrayList<>();
        if (!appendTableFiles.isEmpty()) {
            msg.add(appendTableFiles.size() + " append table files");
        }
        if (!appendChangelog.isEmpty()) {
            msg.add(appendChangelog.size() + " append Changelogs");
        }
        if (!appendIndexFiles.isEmpty()) {
            msg.add(appendIndexFiles.size() + " append index files");
        }
        if (!compactTableFiles.isEmpty()) {
            msg.add(compactTableFiles.size() + " compact table files");
        }
        if (!compactChangelog.isEmpty()) {
            msg.add(compactChangelog.size() + " compact Changelogs");
        }
        if (!compactIndexFiles.isEmpty()) {
            msg.add(compactIndexFiles.size() + " compact index files");
        }
        return String.join(", ", msg);
    }

    public static List<BinaryRow> changedPartitions(
            List<ManifestEntry> appendTableFiles,
            List<ManifestEntry> compactTableFiles,
            List<IndexManifestEntry> appendIndexFiles) {
        Set<BinaryRow> changedPartitions = new HashSet<>();
        for (ManifestEntry appendTableFile : appendTableFiles) {
            changedPartitions.add(appendTableFile.partition());
        }
        for (ManifestEntry compactTableFile : compactTableFiles) {
            changedPartitions.add(compactTableFile.partition());
        }
        for (IndexManifestEntry appendIndexFile : appendIndexFiles) {
            if (appendIndexFile.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                changedPartitions.add(appendIndexFile.partition());
            }
        }
        return new ArrayList<>(changedPartitions);
    }
}
