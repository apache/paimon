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

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Drops complete compaction messages, preserving their inputs for a later attempt. */
public class DiscardingCompactChangesProvider implements CommitChangesProvider {
    private final CommitChangesProvider delegate;
    private final List<List<ManifestEntry>> tasks;
    private final Set<FileEntry.Identifier> discarded = new HashSet<>();

    public DiscardingCompactChangesProvider(
            CommitChangesProvider delegate, List<List<ManifestEntry>> tasks) {
        this.delegate = delegate;
        this.tasks = tasks;
    }

    public boolean discard(List<FileEntry.Identifier> conflicts) {
        Set<FileEntry.Identifier> identifiers = new HashSet<>(conflicts);
        int previous = discarded.size();
        for (List<ManifestEntry> task : tasks) {
            if (task.stream()
                    .anyMatch(
                            entry ->
                                    entry.kind() == FileKind.ADD
                                            && identifiers.contains(entry.identifier()))) {
                task.forEach(entry -> discarded.add(entry.identifier()));
            }
        }
        return discarded.size() > previous;
    }

    @Override
    public CommitChanges provide(@Nullable Snapshot latest) {
        CommitChanges changes = delegate.provide(latest);
        List<ManifestEntry> retained = new ArrayList<>();
        for (ManifestEntry entry : changes.tableFiles) {
            if (!discarded.contains(entry.identifier())) {
                retained.add(entry);
            }
        }
        return new CommitChanges(retained, changes.changelogFiles, changes.indexFiles);
    }

    public List<ManifestEntry> retainedOriginalFiles() {
        return tasks.stream()
                .flatMap(List::stream)
                .filter(entry -> !discarded.contains(entry.identifier()))
                .collect(Collectors.toList());
    }

    @Override
    public Set<Long> rebasedReassignments() {
        return delegate.rebasedReassignments();
    }
}
