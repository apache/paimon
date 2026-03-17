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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Compact coordinator for BTree global index.
 *
 * <p>This class is responsible for planning compaction tasks:
 *
 * <ul>
 *   <li>Group entries by partition and bucket.
 *   <li>Filter by min-files threshold per group.
 *   <li>Identify contiguous row ranges that overlap.
 *   <li>Generate {@link BTreeGlobalIndexCompactTask} for each mergeable segment.
 * </ul>
 */
public class BTreeGlobalIndexCompactCoordinator {

    private final int minFiles;

    public BTreeGlobalIndexCompactCoordinator(Options options) {
        this.minFiles = options.get(BTreeIndexOptions.BTREE_INDEX_COMPACTION_MIN_FILES);
        checkArgument(minFiles >= 2, "btree-index.compaction.min-files must be >= 2.");
    }

    public int minFiles() {
        return minFiles;
    }

    /**
     * Plan compaction tasks from the given index manifest entries.
     *
     * <p>The method groups entries by partition and bucket, then within each group identifies
     * contiguous segments of overlapping row ranges. Only segments with more than one file produce
     * a compaction task.
     *
     * <p>Groups with fewer files than {@link #minFiles()} are skipped entirely.
     *
     * @param entries all current index manifest entries for one index type
     * @return a list of compaction tasks, each representing one segment to merge
     */
    public List<BTreeGlobalIndexCompactTask> plan(List<IndexManifestEntry> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }

        String expectedIndexType = "btree";
        Integer expectedIndexFieldId = entries.get(0).indexFile().globalIndexMeta().indexFieldId();
        Map<String, List<IndexManifestEntry>> grouped = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(
                    expectedIndexType.equals(entry.indexFile().indexType()),
                    "All entries for compaction should have the same index type, "
                            + "expected '%s' but got '%s'.",
                    expectedIndexType,
                    entry.indexFile().indexType());
            checkArgument(
                    expectedIndexFieldId.equals(meta.indexFieldId()),
                    "All entries for compaction should have the same index field id, "
                            + "expected '%s' but got '%s'.",
                    expectedIndexFieldId,
                    meta.indexFieldId());

            String groupKey = entry.partition().toString() + "|" + entry.bucket();
            grouped.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(entry);
        }

        List<BTreeGlobalIndexCompactTask> tasks = new ArrayList<>();
        for (List<IndexManifestEntry> files : grouped.values()) {
            if (files.size() < minFiles) {
                continue;
            }
            tasks.addAll(planGroupedFiles(files, expectedIndexType, expectedIndexFieldId));
        }
        return tasks;
    }

    /**
     * Check whether the given entries meet the min-files threshold for compaction.
     *
     * @param entries all current index manifest entries
     * @return true if at least one group meets the min-files threshold
     */
    public boolean meetsMinFilesThreshold(List<IndexManifestEntry> entries) {
        Map<String, Integer> groupCounts = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            if (entry.indexFile().globalIndexMeta() == null) {
                continue;
            }
            String groupKey = entry.partition().toString() + "|" + entry.bucket();
            groupCounts.merge(groupKey, 1, Integer::sum);
        }
        for (int count : groupCounts.values()) {
            if (count >= minFiles) {
                return true;
            }
        }
        return false;
    }

    private List<BTreeGlobalIndexCompactTask> planGroupedFiles(
            List<IndexManifestEntry> files, String indexType, int indexFieldId) {
        IndexManifestEntry firstFile = files.get(0);

        LinkedHashMap<Range, List<IndexManifestEntry>> rangeGroups = new LinkedHashMap<>();
        for (IndexManifestEntry file : files) {
            Range range = rowRange(file);
            rangeGroups.computeIfAbsent(range, k -> new ArrayList<>()).add(file);
        }
        List<Map.Entry<Range, List<IndexManifestEntry>>> sortedRangeGroups =
                new ArrayList<>(rangeGroups.entrySet());
        sortedRangeGroups.sort(Comparator.comparingLong(e -> e.getKey().from));

        List<BTreeGlobalIndexCompactTask> tasks = new ArrayList<>();
        LinkedHashMap<Range, List<IndexManifestEntry>> currentSegment = new LinkedHashMap<>();
        Range currentMerged = null;
        int currentFileCount = 0;

        for (Map.Entry<Range, List<IndexManifestEntry>> rangeGroup : sortedRangeGroups) {
            Range range = rangeGroup.getKey();
            if (currentMerged == null) {
                currentSegment.put(range, rangeGroup.getValue());
                currentMerged = range;
                currentFileCount += rangeGroup.getValue().size();
                continue;
            }

            Range merged = Range.union(currentMerged, range);
            if (merged != null) {
                currentSegment.put(range, rangeGroup.getValue());
                currentMerged = merged;
                currentFileCount += rangeGroup.getValue().size();
            } else {
                if (currentFileCount >= minFiles) {
                    tasks.add(
                            createTask(
                                    firstFile,
                                    indexType,
                                    indexFieldId,
                                    currentSegment,
                                    currentMerged));
                }
                currentSegment = new LinkedHashMap<>();
                currentSegment.put(range, rangeGroup.getValue());
                currentMerged = range;
                currentFileCount = rangeGroup.getValue().size();
            }
        }

        if (!currentSegment.isEmpty() && currentFileCount >= minFiles) {
            tasks.add(
                    createTask(firstFile, indexType, indexFieldId, currentSegment, currentMerged));
        }
        return tasks;
    }

    private BTreeGlobalIndexCompactTask createTask(
            IndexManifestEntry firstFile,
            String indexType,
            int indexFieldId,
            LinkedHashMap<Range, List<IndexManifestEntry>> rangeGroups,
            Range mergedRange) {
        List<BTreeGlobalIndexCompactTask.RangeGroup> groups = new ArrayList<>();
        for (Map.Entry<Range, List<IndexManifestEntry>> entry : rangeGroups.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                groups.add(
                        new BTreeGlobalIndexCompactTask.RangeGroup(
                                entry.getKey(), new ArrayList<>(entry.getValue())));
            }
        }
        return new BTreeGlobalIndexCompactTask(
                firstFile.partition(),
                firstFile.bucket(),
                indexType,
                indexFieldId,
                groups,
                mergedRange);
    }

    private Range rowRange(IndexManifestEntry entry) {
        GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
        checkArgument(meta != null, "Global index meta must not be null.");
        return new Range(meta.rowRangeStart(), meta.rowRangeEnd());
    }
}
