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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Immutable sorted-index state for one field and bucket. */
public final class PkSortedBucketIndexState {

    private final List<PkSortedIndexGroup> groups;
    private final List<PrimaryKeyIndexSourceFile> coveredSourceFiles;
    private final List<PrimaryKeyIndexSourceFile> uncoveredSourceFiles;
    private final List<IndexFileMeta> rejectedPayloads;

    private PkSortedBucketIndexState(
            List<PkSortedIndexGroup> groups,
            List<PrimaryKeyIndexSourceFile> coveredSourceFiles,
            List<PrimaryKeyIndexSourceFile> uncoveredSourceFiles,
            List<IndexFileMeta> rejectedPayloads) {
        this.groups = Collections.unmodifiableList(groups);
        this.coveredSourceFiles = Collections.unmodifiableList(coveredSourceFiles);
        this.uncoveredSourceFiles = Collections.unmodifiableList(uncoveredSourceFiles);
        this.rejectedPayloads = Collections.unmodifiableList(rejectedPayloads);
    }

    public static PkSortedBucketIndexState fromActivePayloads(
            int fieldId,
            String indexType,
            List<PrimaryKeyIndexSourceFile> activeSourceFiles,
            List<IndexFileMeta> activePayloads) {
        Map<List<PrimaryKeyIndexSourceFile>, List<IndexFileMeta>> payloadsBySources =
                new LinkedHashMap<>();
        List<IndexFileMeta> rejected = new ArrayList<>();
        for (IndexFileMeta payload : activePayloads) {
            try {
                List<PrimaryKeyIndexSourceFile> sourceFiles =
                        PrimaryKeyIndexSourceMeta.fromIndexFile(payload).sourceFiles();
                payloadsBySources
                        .computeIfAbsent(sourceFiles, key -> new ArrayList<>())
                        .add(payload);
            } catch (RuntimeException ignored) {
                rejected.add(payload);
            }
        }

        List<PkSortedIndexGroup> groups = new ArrayList<>();
        Set<PrimaryKeyIndexSourceFile> activeSet = new HashSet<>(activeSourceFiles);
        Set<PrimaryKeyIndexSourceFile> coveredSet = new HashSet<>();
        for (Map.Entry<List<PrimaryKeyIndexSourceFile>, List<IndexFileMeta>> entry :
                payloadsBySources.entrySet()) {
            Optional<PkSortedIndexGroup> group =
                    PkSortedIndexGroup.create(fieldId, indexType, entry.getKey(), entry.getValue());
            boolean overlapsActiveSource = false;
            for (PrimaryKeyIndexSourceFile sourceFile : entry.getKey()) {
                if (activeSet.contains(sourceFile) && coveredSet.contains(sourceFile)) {
                    overlapsActiveSource = true;
                    break;
                }
            }
            if (group.isPresent() && !overlapsActiveSource) {
                groups.add(group.get());
                coveredSet.addAll(entry.getKey());
            } else {
                rejected.addAll(entry.getValue());
            }
        }

        List<PrimaryKeyIndexSourceFile> covered = new ArrayList<>();
        List<PrimaryKeyIndexSourceFile> uncovered = new ArrayList<>();
        for (PrimaryKeyIndexSourceFile sourceFile : activeSourceFiles) {
            if (coveredSet.contains(sourceFile)) {
                covered.add(sourceFile);
            } else {
                uncovered.add(sourceFile);
            }
        }
        return new PkSortedBucketIndexState(groups, covered, uncovered, rejected);
    }

    public List<PkSortedIndexGroup> groups() {
        return groups;
    }

    public List<PrimaryKeyIndexSourceFile> coveredSourceFiles() {
        return coveredSourceFiles;
    }

    public List<PrimaryKeyIndexSourceFile> uncoveredSourceFiles() {
        return uncoveredSourceFiles;
    }

    public List<IndexFileMeta> rejectedPayloads() {
        return rejectedPayloads;
    }
}
