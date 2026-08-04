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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.utils.Pair;

import org.apache.flink.table.connector.RowLevelModificationScanContext;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Snapshots used by Paimon sources in a row-level modification statement. */
public class DataEvolutionRowLevelModificationScanContext
        implements RowLevelModificationScanContext {

    public static final long EMPTY_TABLE_SNAPSHOT = -1L;

    private static final ThreadLocal<Map<Pair<String, String>, Long>> FALLBACK_SNAPSHOT_IDS =
            ThreadLocal.withInitial(HashMap::new);

    private final Map<Pair<String, String>, Long> snapshotIds;

    private DataEvolutionRowLevelModificationScanContext(
            Map<Pair<String, String>, Long> snapshotIds) {
        this.snapshotIds = Collections.unmodifiableMap(snapshotIds);
    }

    public static DataEvolutionRowLevelModificationScanContext addSnapshot(
            @Nullable RowLevelModificationScanContext previous,
            String tableLocation,
            String branch,
            long snapshotId) {
        Map<Pair<String, String>, Long> snapshotIds = new HashMap<>();
        if (previous instanceof DataEvolutionRowLevelModificationScanContext) {
            snapshotIds.putAll(
                    ((DataEvolutionRowLevelModificationScanContext) previous).snapshotIds);
        }
        snapshotIds.put(Pair.of(tableLocation, branch), snapshotId);
        return new DataEvolutionRowLevelModificationScanContext(snapshotIds);
    }

    @Nullable
    public static Long snapshotId(
            @Nullable RowLevelModificationScanContext context,
            String tableLocation,
            String branch) {
        if (!(context instanceof DataEvolutionRowLevelModificationScanContext)) {
            return null;
        }
        return ((DataEvolutionRowLevelModificationScanContext) context)
                .snapshotIds.get(Pair.of(tableLocation, branch));
    }

    /**
     * Registers a planned snapshot for planners which do not forward the row-level modification
     * context from source to sink.
     *
     * <p>The fallback is thread-confined and consumed once. It never derives a snapshot from the
     * sink-side table, so a concurrent commit cannot move the DELETE to a newer snapshot.
     */
    public static void registerFallbackSnapshot(
            String tableLocation, String branch, long snapshotId) {
        FALLBACK_SNAPSHOT_IDS.get().put(Pair.of(tableLocation, branch), snapshotId);
    }

    @Nullable
    public static Long takeFallbackSnapshot(String tableLocation, String branch) {
        Map<Pair<String, String>, Long> snapshotIds = FALLBACK_SNAPSHOT_IDS.get();
        Long snapshotId = snapshotIds.remove(Pair.of(tableLocation, branch));
        removeFallbackIfEmpty(snapshotIds);
        return snapshotId;
    }

    public static void clearFallbackSnapshot(String tableLocation, String branch) {
        Map<Pair<String, String>, Long> snapshotIds = FALLBACK_SNAPSHOT_IDS.get();
        snapshotIds.remove(Pair.of(tableLocation, branch));
        removeFallbackIfEmpty(snapshotIds);
    }

    private static void removeFallbackIfEmpty(Map<Pair<String, String>, Long> snapshotIds) {
        if (snapshotIds.isEmpty()) {
            FALLBACK_SNAPSHOT_IDS.remove();
        }
    }
}
