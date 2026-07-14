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

package org.apache.paimon.index.pk;

import org.apache.paimon.io.DataFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Derives logical compaction levels from immutable primary-key index source metadata. */
public final class PrimaryKeyIndexLevels<T> {

    private final int fanout;
    private final double staleRatioThreshold;
    private final Function<T, String> identity;
    private final Function<T, List<PrimaryKeyIndexSourceFile>> sources;

    public PrimaryKeyIndexLevels(
            int fanout,
            double staleRatioThreshold,
            Function<T, String> identity,
            Function<T, List<PrimaryKeyIndexSourceFile>> sources) {
        checkArgument(fanout > 1, "Primary-key index level fanout must be greater than one.");
        checkArgument(
                staleRatioThreshold > 0 && staleRatioThreshold <= 1,
                "Primary-key index stale ratio threshold must be in (0, 1].");
        this.fanout = fanout;
        this.staleRatioThreshold = staleRatioThreshold;
        this.identity = identity;
        this.sources = sources;
    }

    public Optional<Plan<T>> pick(List<T> units, Map<String, DataFileMeta> activeSourceFiles) {
        T staleCandidate = null;
        double highestStaleRatio = -1;
        for (T unit : units) {
            double staleRatio = staleRatio(unit, activeSourceFiles);
            if (staleRatio >= staleRatioThreshold
                    && (staleRatio > highestStaleRatio
                            || (staleRatio == highestStaleRatio
                                    && (staleCandidate == null
                                            || identity.apply(unit)
                                                            .compareTo(
                                                                    identity.apply(staleCandidate))
                                                    < 0)))) {
                staleCandidate = unit;
                highestStaleRatio = staleRatio;
            }
        }
        if (staleCandidate != null) {
            return Optional.of(
                    createPlan(Collections.singletonList(staleCandidate), activeSourceFiles));
        }

        List<T> candidates = new ArrayList<>(units);
        candidates.sort(
                Comparator.comparingLong(this::buildRowCount).thenComparing(identity::apply));
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

    private double staleRatio(T unit, Map<String, DataFileMeta> activeSourceFiles) {
        long totalRows = 0;
        long staleRows = 0;
        for (PrimaryKeyIndexSourceFile source : sources.apply(unit)) {
            totalRows = Math.addExact(totalRows, source.rowCount());
            DataFileMeta active = activeSourceFiles.get(source.fileName());
            if (active == null) {
                staleRows = Math.addExact(staleRows, source.rowCount());
            } else {
                checkArgument(
                        active.rowCount() == source.rowCount(),
                        "Primary-key index source %s row count does not match active data file.",
                        source.fileName());
            }
        }
        return totalRows == 0 ? 0 : ((double) staleRows) / totalRows;
    }

    private Plan<T> createPlan(List<T> inputUnits, Map<String, DataFileMeta> activeSourceFiles) {
        Map<String, DataFileMeta> selectedSources = new TreeMap<>();
        for (T unit : inputUnits) {
            for (PrimaryKeyIndexSourceFile source : sources.apply(unit)) {
                DataFileMeta active = activeSourceFiles.get(source.fileName());
                if (active != null) {
                    checkArgument(
                            active.rowCount() == source.rowCount(),
                            "Primary-key index source %s row count does not match active data file.",
                            source.fileName());
                    selectedSources.put(active.fileName(), active);
                }
            }
        }
        return new Plan<>(inputUnits, new ArrayList<>(selectedSources.values()));
    }

    private long buildRowCount(T unit) {
        long rowCount = 0;
        for (PrimaryKeyIndexSourceFile source : sources.apply(unit)) {
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

    /** A deterministic primary-key index rebuild selection. */
    public static final class Plan<T> {

        private final List<T> inputUnits;
        private final List<DataFileMeta> sourceFiles;

        private Plan(List<T> inputUnits, List<DataFileMeta> sourceFiles) {
            this.inputUnits = Collections.unmodifiableList(new ArrayList<>(inputUnits));
            this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        }

        public List<T> inputUnits() {
            return inputUnits;
        }

        public List<DataFileMeta> sourceFiles() {
            return sourceFiles;
        }
    }
}
