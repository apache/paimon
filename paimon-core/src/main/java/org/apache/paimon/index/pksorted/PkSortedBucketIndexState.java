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
import org.apache.paimon.index.pk.PrimaryKeyIndexSourcePolicy;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

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

    public static PkSortedBucketIndexState fromActiveDataFiles(
            int fieldId,
            String indexType,
            List<DataFileMeta> activeDataFiles,
            List<IndexFileMeta> activePayloads) {
        Map<Integer, List<PrimaryKeyIndexSourceFile>> sourcesByLevel = new TreeMap<>();
        for (DataFileMeta dataFile : activeDataFiles) {
            if (PrimaryKeyIndexSourcePolicy.shouldRead(dataFile)) {
                sourcesByLevel
                        .computeIfAbsent(dataFile.level(), ignored -> new ArrayList<>())
                        .add(
                                new PrimaryKeyIndexSourceFile(
                                        dataFile.fileName(), dataFile.rowCount()));
            }
        }
        for (List<PrimaryKeyIndexSourceFile> sources : sourcesByLevel.values()) {
            sources.sort(Comparator.comparing(PrimaryKeyIndexSourceFile::fileName));
        }

        Map<Integer, List<IndexFileMeta>> payloadsByLevel = new TreeMap<>();
        List<IndexFileMeta> rejected = new ArrayList<>();
        for (IndexFileMeta payload : activePayloads) {
            try {
                PrimaryKeyIndexSourceMeta sourceMeta =
                        PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
                List<PrimaryKeyIndexSourceFile> desired =
                        sourcesByLevel.get(sourceMeta.dataLevel());
                if (desired == null || !desired.equals(sourceMeta.sourceFiles())) {
                    rejected.add(payload);
                } else {
                    payloadsByLevel
                            .computeIfAbsent(sourceMeta.dataLevel(), ignored -> new ArrayList<>())
                            .add(payload);
                }
            } catch (RuntimeException ignored) {
                rejected.add(payload);
            }
        }

        List<PkSortedIndexGroup> groups = new ArrayList<>();
        Set<Integer> coveredLevels = new HashSet<>();
        for (Map.Entry<Integer, List<IndexFileMeta>> entry : payloadsByLevel.entrySet()) {
            List<IndexFileMeta> levelPayloads = entry.getValue();
            Optional<PkSortedIndexGroup> group =
                    levelPayloads.size() == 1
                            ? PkSortedIndexGroup.create(
                                    fieldId,
                                    indexType,
                                    sourcesByLevel.get(entry.getKey()),
                                    levelPayloads)
                            : Optional.empty();
            if (group.isPresent()) {
                groups.add(group.get());
                coveredLevels.add(entry.getKey());
            } else {
                rejected.addAll(levelPayloads);
            }
        }

        List<PrimaryKeyIndexSourceFile> covered = new ArrayList<>();
        List<PrimaryKeyIndexSourceFile> uncovered = new ArrayList<>();
        for (Map.Entry<Integer, List<PrimaryKeyIndexSourceFile>> entry :
                sourcesByLevel.entrySet()) {
            (coveredLevels.contains(entry.getKey()) ? covered : uncovered).addAll(entry.getValue());
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
