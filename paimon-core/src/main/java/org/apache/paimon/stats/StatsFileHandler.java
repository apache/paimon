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

package org.apache.paimon.stats;

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.utils.SnapshotManager;

import java.util.Optional;

/** Handler of StatsFile. */
public class StatsFileHandler {

    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    private final StatsFile statsFile;

    public StatsFileHandler(
            SnapshotManager snapshotManager, SchemaManager schemaManager, StatsFile statsFile) {
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.statsFile = statsFile;
    }

    /**
     * Write stats to a stats file.
     *
     * @return the written file name
     */
    public String writeStats(Statistics stats) {
        stats.serializeFieldsToString(schemaManager.schema(stats.schemaId()));
        return statsFile.write(stats);
    }

    /**
     * Read stats of the latest snapshot.
     *
     * @return stats
     */
    public Optional<Statistics> readStats() {
        Long latestSnapshotId = snapshotManager.latestSnapshotId();
        if (latestSnapshotId == null) {
            throw new IllegalStateException("Unable to obtain the latest snapshot");
        }
        return readStats(latestSnapshotId);
    }

    /**
     * Read stats of the specified snapshot.
     *
     * @return stats
     */
    public Optional<Statistics> readStats(long snapshotId) {
        return readStats(snapshotManager.snapshot(snapshotId));
    }

    public Optional<Statistics> readStats(Snapshot snapshot) {
        if (snapshot.statistics() == null) {
            return Optional.empty();
        } else {
            Statistics stats = statsFile.read(snapshot.statistics());
            stats.deserializeFieldsFromString(schemaManager.schema(stats.schemaId()));
            return Optional.of(stats);
        }
    }

    /** Delete stats of the specified snapshot. */
    public void deleteStats(long snapshotId) {
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.statistics() != null) {
            statsFile.delete(snapshot.statistics());
        }
    }

    public void deleteStats(String statistic) {
        statsFile.delete(statistic);
    }
}
