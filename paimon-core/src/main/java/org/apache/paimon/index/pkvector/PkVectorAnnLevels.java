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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Derives logical ANN compaction levels from immutable segment source metadata. */
final class PkVectorAnnLevels {

    private final int fanout;
    private final double staleRatioThreshold;

    PkVectorAnnLevels(int fanout, double staleRatioThreshold) {
        checkArgument(fanout > 1, "ANN level fanout must be greater than one.");
        checkArgument(
                staleRatioThreshold > 0 && staleRatioThreshold <= 1,
                "ANN stale ratio threshold must be in (0, 1].");
        this.fanout = fanout;
        this.staleRatioThreshold = staleRatioThreshold;
    }

    Optional<Plan> pick(List<IndexFileMeta> segments, Map<String, DataFileMeta> activeSourceFiles) {
        IndexFileMeta staleCandidate = null;
        double highestStaleRatio = -1;
        for (IndexFileMeta segment : segments) {
            double staleRatio = staleRatio(segment, activeSourceFiles);
            if (staleRatio >= staleRatioThreshold
                    && (staleRatio > highestStaleRatio
                            || (staleRatio == highestStaleRatio
                                    && (staleCandidate == null
                                            || segment.fileName()
                                                            .compareTo(staleCandidate.fileName())
                                                    < 0)))) {
                staleCandidate = segment;
                highestStaleRatio = staleRatio;
            }
        }
        if (staleCandidate != null) {
            return Optional.of(
                    createPlan(Collections.singletonList(staleCandidate), activeSourceFiles));
        }

        List<IndexFileMeta> candidates = new ArrayList<>(segments);
        candidates.sort(
                Comparator.comparingLong(PkVectorAnnLevels::buildRowCount)
                        .thenComparing(IndexFileMeta::fileName));
        for (int start = 0; start + fanout <= candidates.size(); start++) {
            long smallest = buildRowCount(candidates.get(start));
            long largest = buildRowCount(candidates.get(start + fanout - 1));
            if (largest <= saturatedMultiply(smallest, fanout)) {
                return Optional.of(
                        createPlan(
                                new ArrayList<>(candidates.subList(start, start + fanout)),
                                activeSourceFiles));
            }
        }
        return Optional.empty();
    }

    private static double staleRatio(
            IndexFileMeta segment, Map<String, DataFileMeta> activeSourceFiles) {
        long totalRows = 0;
        long staleRows = 0;
        for (PrimaryKeyIndexSourceFile source :
                PrimaryKeyIndexSourceMeta.fromIndexFile(segment).sourceFiles()) {
            totalRows = Math.addExact(totalRows, source.rowCount());
            DataFileMeta active = activeSourceFiles.get(source.fileName());
            if (active == null) {
                staleRows = Math.addExact(staleRows, source.rowCount());
            } else {
                checkArgument(
                        active.rowCount() == source.rowCount(),
                        "ANN source %s row count does not match active data file.",
                        source.fileName());
            }
        }
        return totalRows == 0 ? 0 : ((double) staleRows) / totalRows;
    }

    private static Plan createPlan(
            List<IndexFileMeta> inputSegments, Map<String, DataFileMeta> activeSourceFiles) {
        Map<String, DataFileMeta> selectedSources = new TreeMap<>();
        for (IndexFileMeta segment : inputSegments) {
            for (PrimaryKeyIndexSourceFile source :
                    PrimaryKeyIndexSourceMeta.fromIndexFile(segment).sourceFiles()) {
                DataFileMeta active = activeSourceFiles.get(source.fileName());
                if (active != null) {
                    checkArgument(
                            active.rowCount() == source.rowCount(),
                            "ANN source %s row count does not match active data file.",
                            source.fileName());
                    selectedSources.put(active.fileName(), active);
                }
            }
        }
        return new Plan(inputSegments, new ArrayList<>(selectedSources.values()));
    }

    private static long buildRowCount(IndexFileMeta segment) {
        long rowCount = 0;
        for (PrimaryKeyIndexSourceFile source :
                PrimaryKeyIndexSourceMeta.fromIndexFile(segment).sourceFiles()) {
            rowCount = Math.addExact(rowCount, source.rowCount());
        }
        return rowCount;
    }

    private static long saturatedMultiply(long value, int multiplier) {
        if (value > Long.MAX_VALUE / multiplier) {
            return Long.MAX_VALUE;
        }
        return value * multiplier;
    }

    /** A deterministic ANN rebuild selection. */
    static final class Plan {

        private final List<IndexFileMeta> inputSegments;
        private final List<DataFileMeta> sourceFiles;

        private Plan(List<IndexFileMeta> inputSegments, List<DataFileMeta> sourceFiles) {
            this.inputSegments = Collections.unmodifiableList(new ArrayList<>(inputSegments));
            this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        }

        List<IndexFileMeta> inputSegments() {
            return inputSegments;
        }

        List<DataFileMeta> sourceFiles() {
            return sourceFiles;
        }
    }
}
