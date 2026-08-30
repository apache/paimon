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

    private static final class PayloadCandidate {

        private final IndexFileMeta payload;
        private final PkSortedIndexGroup group;
        private final List<PrimaryKeyIndexSourceFile> activeSources;

        private PayloadCandidate(
                IndexFileMeta payload,
                PkSortedIndexGroup group,
                List<PrimaryKeyIndexSourceFile> activeSources) {
            this.payload = payload;
            this.group = group;
            this.activeSources = activeSources;
        }
    }

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

        Map<Integer, List<PayloadCandidate>> candidatesByLevel = new TreeMap<>();
        List<IndexFileMeta> rejected = new ArrayList<>();
        for (IndexFileMeta payload : activePayloads) {
            try {
                PrimaryKeyIndexSourceMeta sourceMeta =
                        PrimaryKeyIndexSourceMeta.fromIndexFile(payload);
                List<PrimaryKeyIndexSourceFile> activeLevelSources =
                        sourcesByLevel.get(sourceMeta.dataLevel());
                if (activeLevelSources == null) {
                    rejected.add(payload);
                    continue;
                }

                List<PrimaryKeyIndexSourceFile> payloadSources = sourceMeta.sourceFiles();
                List<PrimaryKeyIndexSourceFile> activeIntersection =
                        activeIntersection(activeLevelSources, payloadSources);
                if (activeIntersection == null || activeIntersection.isEmpty()) {
                    rejected.add(payload);
                    continue;
                }

                Optional<PkSortedIndexGroup> group =
                        PkSortedIndexGroup.create(
                                fieldId,
                                indexType,
                                payloadSources,
                                Collections.singletonList(payload));
                if (!group.isPresent()) {
                    rejected.add(payload);
                    continue;
                }
                candidatesByLevel
                        .computeIfAbsent(sourceMeta.dataLevel(), ignored -> new ArrayList<>())
                        .add(new PayloadCandidate(payload, group.get(), activeIntersection));
            } catch (RuntimeException ignored) {
                rejected.add(payload);
            }
        }

        List<PkSortedIndexGroup> groups = new ArrayList<>();
        Map<Integer, Set<PrimaryKeyIndexSourceFile>> coveredSourcesByLevel = new TreeMap<>();
        for (Map.Entry<Integer, List<PayloadCandidate>> entry : candidatesByLevel.entrySet()) {
            List<PayloadCandidate> levelCandidates = entry.getValue();
            if (levelCandidates.size() != 1) {
                for (PayloadCandidate candidate : levelCandidates) {
                    rejected.add(candidate.payload);
                }
                continue;
            }
            PayloadCandidate candidate = levelCandidates.get(0);
            groups.add(candidate.group);
            coveredSourcesByLevel
                    .computeIfAbsent(entry.getKey(), ignored -> new HashSet<>())
                    .addAll(candidate.activeSources);
        }

        List<PrimaryKeyIndexSourceFile> covered = new ArrayList<>();
        List<PrimaryKeyIndexSourceFile> uncovered = new ArrayList<>();
        for (Map.Entry<Integer, List<PrimaryKeyIndexSourceFile>> entry :
                sourcesByLevel.entrySet()) {
            Set<PrimaryKeyIndexSourceFile> coveredSources =
                    coveredSourcesByLevel.getOrDefault(entry.getKey(), Collections.emptySet());
            for (PrimaryKeyIndexSourceFile source : entry.getValue()) {
                (coveredSources.contains(source) ? covered : uncovered).add(source);
            }
        }
        return new PkSortedBucketIndexState(groups, covered, uncovered, rejected);
    }

    private static List<PrimaryKeyIndexSourceFile> activeIntersection(
            List<PrimaryKeyIndexSourceFile> activeSources,
            List<PrimaryKeyIndexSourceFile> payloadSources) {
        List<PrimaryKeyIndexSourceFile> intersection = new ArrayList<>();
        int activeSourceIndex = 0;
        for (int i = 0; i < payloadSources.size(); i++) {
            PrimaryKeyIndexSourceFile source = payloadSources.get(i);
            if (i > 0 && payloadSources.get(i - 1).fileName().compareTo(source.fileName()) >= 0) {
                return null;
            }
            while (activeSourceIndex < activeSources.size()
                    && activeSources.get(activeSourceIndex).fileName().compareTo(source.fileName())
                            < 0) {
                activeSourceIndex++;
            }
            if (activeSourceIndex == activeSources.size()
                    || !activeSources.get(activeSourceIndex).fileName().equals(source.fileName())) {
                continue;
            }
            if (activeSources.get(activeSourceIndex).rowCount() != source.rowCount()) {
                return null;
            }
            intersection.add(source);
        }
        return intersection;
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
