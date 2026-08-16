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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Derives logical compaction levels from immutable primary-key index source metadata. */
public final class PrimaryKeyIndexLevels<T> {

    private final Function<T, Integer> dataLevel;
    private final Function<T, List<PrimaryKeyIndexSourceFile>> sources;

    public PrimaryKeyIndexLevels(
            Function<T, Integer> dataLevel, Function<T, List<PrimaryKeyIndexSourceFile>> sources) {
        this.dataLevel = dataLevel;
        this.sources = sources;
    }

    public Optional<Plan<T>> pick(List<T> units, Map<String, DataFileMeta> activeSourceFiles) {
        Map<Integer, List<DataFileMeta>> desiredByLevel = new TreeMap<>();
        for (DataFileMeta file : activeSourceFiles.values()) {
            if (file.level() > 0) {
                desiredByLevel
                        .computeIfAbsent(file.level(), ignored -> new ArrayList<>())
                        .add(file);
            }
        }
        for (List<DataFileMeta> files : desiredByLevel.values()) {
            files.sort(Comparator.comparing(DataFileMeta::fileName));
        }

        Map<Integer, T> unitsByLevel = new TreeMap<>();
        for (T unit : units) {
            int level = dataLevel.apply(unit);
            checkArgument(level > 0, "Primary-key index data level must be positive.");
            checkArgument(
                    unitsByLevel.put(level, unit) == null,
                    "Multiple primary-key index units exist for data level %s.",
                    level);
        }

        Set<Integer> levels = new TreeSet<>();
        levels.addAll(desiredByLevel.keySet());
        levels.addAll(unitsByLevel.keySet());
        for (int level : levels) {
            T unit = unitsByLevel.get(level);
            List<DataFileMeta> desired =
                    desiredByLevel.getOrDefault(level, Collections.emptyList());
            if (unit == null) {
                return Optional.of(new Plan<>(level, Collections.emptyList(), desired));
            }
            if (!matches(sources.apply(unit), desired)) {
                return Optional.of(new Plan<>(level, Collections.singletonList(unit), desired));
            }
        }
        return Optional.empty();
    }

    public boolean isCurrent(Plan<T> plan, Map<String, DataFileMeta> activeSourceFiles) {
        List<DataFileMeta> current = new ArrayList<>();
        for (DataFileMeta file : activeSourceFiles.values()) {
            if (file.level() == plan.dataLevel()) {
                current.add(file);
            }
        }
        current.sort(Comparator.comparing(DataFileMeta::fileName));
        if (plan.sourceFiles().size() != current.size()) {
            return false;
        }
        for (int i = 0; i < current.size(); i++) {
            DataFileMeta planned = plan.sourceFiles().get(i);
            DataFileMeta actual = current.get(i);
            if (!planned.fileName().equals(actual.fileName())
                    || planned.rowCount() != actual.rowCount()) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(
            List<PrimaryKeyIndexSourceFile> sources, List<DataFileMeta> desired) {
        if (sources.size() != desired.size()) {
            return false;
        }
        for (int i = 0; i < sources.size(); i++) {
            PrimaryKeyIndexSourceFile source = sources.get(i);
            DataFileMeta file = desired.get(i);
            if (!source.fileName().equals(file.fileName())
                    || source.rowCount() != file.rowCount()) {
                return false;
            }
        }
        return true;
    }

    /** A deterministic primary-key index rebuild selection. */
    public static final class Plan<T> {

        private final int dataLevel;
        private final List<T> inputUnits;
        private final List<DataFileMeta> sourceFiles;

        private Plan(int dataLevel, List<T> inputUnits, List<DataFileMeta> sourceFiles) {
            this.dataLevel = dataLevel;
            this.inputUnits = Collections.unmodifiableList(new ArrayList<>(inputUnits));
            this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        }

        public int dataLevel() {
            return dataLevel;
        }

        public List<T> inputUnits() {
            return inputUnits;
        }

        public List<DataFileMeta> sourceFiles() {
            return sourceFiles;
        }
    }
}
