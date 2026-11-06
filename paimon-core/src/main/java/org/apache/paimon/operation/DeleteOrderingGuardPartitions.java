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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import javax.annotation.Nullable;

import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.paimon.index.DeleteOrderingGuardFile.DELETE_ORDERING_GUARD;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Snapshot-pinned layout and logical update time of partitions retaining hidden guards. */
public class DeleteOrderingGuardPartitions {

    public static final String SNAPSHOT_PROPERTY = "delete-ordering-guards.partitions";
    public static final int UNKNOWN_BUCKETS = 0;
    private static final String VERSION = "version";

    private DeleteOrderingGuardPartitions() {}

    public static Map<BinaryRow, PartitionEntry> read(@Nullable Snapshot snapshot) {
        return entries(readState(snapshot));
    }

    /** Discover legacy hidden partitions without guessing their bucket count or activity time. */
    public static Map<BinaryRow, PartitionEntry> read(
            @Nullable Snapshot snapshot, Supplier<List<IndexManifestEntry>> legacyGuards) {
        return entries(
                !hasState(snapshot) && DeleteOrderingGuardFile.mayContainGuards(snapshot)
                        ? legacyState(legacyGuards.get())
                        : readState(snapshot));
    }

    private static Map<BinaryRow, PartitionEntry> entries(Map<BinaryRow, State> states) {
        Map<BinaryRow, PartitionEntry> result = new LinkedHashMap<>();
        states.forEach(
                (partition, state) ->
                        result.put(
                                partition,
                                new PartitionEntry(
                                        partition, 0, 0, 0, state.updateTime, state.buckets)));
        return result;
    }

    /**
     * Carry logical activity forward without using guard file creation/maintenance time. Only
     * data-file changes can advance it; replacing guard segments leaves it unchanged.
     */
    public static Map<String, String> merge(
            @Nullable Map<String, String> properties,
            @Nullable Snapshot latest,
            List<ManifestEntry> dataChanges,
            List<IndexManifestEntry> indexChanges,
            int buckets,
            boolean rescale,
            Supplier<List<IndexManifestEntry>> legacyGuards) {
        Map<String, String> result =
                properties == null ? new HashMap<>() : new HashMap<>(properties);
        result.remove(SNAPSHOT_PROPERTY);
        boolean tracked = hasState(latest);
        boolean changesGuards =
                indexChanges.stream()
                        .anyMatch(e -> DELETE_ORDERING_GUARD.equals(e.indexFile().indexType()));
        if (!tracked && !changesGuards && !DeleteOrderingGuardFile.mayContainGuards(latest)) {
            return result;
        }

        Map<BinaryRow, State> states = readState(latest);
        if (!tracked && DeleteOrderingGuardFile.mayContainGuards(latest)) {
            // Old segments have no durable activity timestamp. Do not expire them by file mtime:
            // consolidation or copying could make that unrelated to the last logical update.
            states = legacyState(legacyGuards.get());
        }

        Map<BinaryRow, PartitionEntry> data = new HashMap<>();
        PartitionEntry.merge(PartitionEntry.merge(dataChanges), data);
        for (IndexManifestEntry entry : indexChanges) {
            if (DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType())) {
                State state =
                        states.computeIfAbsent(
                                entry.partition(), p -> new State(0, Long.MAX_VALUE, buckets));
                state.files += entry.kind() == FileKind.ADD ? 1 : -1;
                if (rescale && entry.kind() == FileKind.ADD) {
                    state.buckets = buckets;
                }
            }
        }
        states.entrySet().removeIf(e -> e.getValue().files == 0);
        for (Map.Entry<BinaryRow, State> entry : states.entrySet()) {
            State state = entry.getValue();
            checkArgument(state.files > 0, "Invalid delete ordering guard partition file count.");
            PartitionEntry changed = data.get(entry.getKey());
            if (changed != null && !rescale) {
                state.updateTime =
                        state.updateTime == Long.MAX_VALUE
                                ? changed.lastFileCreationTime()
                                : Math.max(state.updateTime, changed.lastFileCreationTime());
                state.buckets = changed.totalBuckets();
            }
        }
        write(result, states);
        return result;
    }

    /** An authoritative manifest replacement inherits activity only for its retained guards. */
    public static Map<String, String> replace(
            Map<String, String> properties, Snapshot previous, List<IndexManifestEntry> guards) {
        Map<BinaryRow, State> previousStates = readState(previous);
        Map<BinaryRow, State> states = new LinkedHashMap<>();
        for (IndexManifestEntry guard : guards) {
            if (DELETE_ORDERING_GUARD.equals(guard.indexFile().indexType())) {
                State state =
                        states.computeIfAbsent(
                                guard.partition(),
                                partition -> {
                                    State old = previousStates.get(partition);
                                    return old == null
                                            ? new State(0, Long.MAX_VALUE, UNKNOWN_BUCKETS)
                                            : new State(0, old.updateTime, old.buckets);
                                });
                state.files++;
            }
        }
        Map<String, String> result = new HashMap<>(properties);
        result.remove(SNAPSHOT_PROPERTY);
        if (hasState(previous) || !states.isEmpty()) {
            write(result, states);
        }
        return result;
    }

    private static boolean hasState(@Nullable Snapshot snapshot) {
        return snapshot != null
                && snapshot.properties() != null
                && snapshot.properties().containsKey(SNAPSHOT_PROPERTY);
    }

    private static Map<BinaryRow, State> legacyState(List<IndexManifestEntry> guards) {
        Map<BinaryRow, State> states = new LinkedHashMap<>();
        for (IndexManifestEntry entry : guards) {
            if (DELETE_ORDERING_GUARD.equals(entry.indexFile().indexType())) {
                states.computeIfAbsent(
                                entry.partition(),
                                p -> new State(0, Long.MAX_VALUE, UNKNOWN_BUCKETS))
                        .files++;
            }
        }
        return states;
    }

    private static Map<BinaryRow, State> readState(@Nullable Snapshot snapshot) {
        Map<BinaryRow, State> result = new LinkedHashMap<>();
        if (!hasState(snapshot)) {
            return result;
        }
        Map<String, String> encoded =
                JsonSerdeUtil.fromJson(
                        snapshot.properties().get(SNAPSHOT_PROPERTY),
                        new TypeReference<LinkedHashMap<String, String>>() {});
        String version = encoded == null ? null : encoded.remove(VERSION);
        checkArgument(
                "1".equals(version) || "2".equals(version),
                "Unsupported delete ordering guard partition state.");
        encoded.forEach(
                (partition, value) -> {
                    String[] fields = value.split(",");
                    checkArgument(fields.length == 3, "Invalid guard partition state.");
                    State state =
                            new State(
                                    Long.parseLong(fields[0]),
                                    Long.parseLong(fields[1]),
                                    Integer.parseInt(fields[2]));
                    checkArgument(
                            state.files > 0
                                    && state.updateTime >= 0
                                    && (state.buckets > 0
                                            || ("2".equals(version)
                                                    && state.buckets == UNKNOWN_BUCKETS)),
                            "Invalid guard partition state.");
                    result.put(deserializeBinaryRow(Base64.getDecoder().decode(partition)), state);
                });
        return result;
    }

    private static void write(Map<String, String> properties, Map<BinaryRow, State> states) {
        Map<String, String> encoded = new LinkedHashMap<>();
        // Readers predating legacy bootstrap must reject unknown layouts instead of assuming
        // that the current schema's bucket count describes every retained partition.
        encoded.put(
                VERSION,
                states.values().stream().anyMatch(state -> state.buckets == UNKNOWN_BUCKETS)
                        ? "2"
                        : "1");
        states.forEach(
                (partition, state) ->
                        encoded.put(
                                Base64.getEncoder().encodeToString(serializeBinaryRow(partition)),
                                state.files + "," + state.updateTime + "," + state.buckets));
        properties.put(SNAPSHOT_PROPERTY, JsonSerdeUtil.toFlatJson(encoded));
    }

    private static class State {
        private long files;
        private long updateTime;
        private int buckets;

        private State(long files, long updateTime, int buckets) {
            this.files = files;
            this.updateTime = updateTime;
            this.buckets = buckets;
        }
    }
}
