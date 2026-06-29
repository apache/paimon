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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Handler of StatsFile. */
public class StatsFileHandler {

    private static final Logger LOG = LoggerFactory.getLogger(StatsFileHandler.class);

    private final SnapshotManager snapshotManager;
    private final SchemaManager schemaManager;
    private final StatsFile statsFile;
    @Nullable private final StatisticsSidecarFile sidecarFile;

    public StatsFileHandler(
            SnapshotManager snapshotManager, SchemaManager schemaManager, StatsFile statsFile) {
        this(snapshotManager, schemaManager, statsFile, null);
    }

    public StatsFileHandler(
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            StatsFile statsFile,
            @Nullable StatisticsSidecarFile sidecarFile) {
        this.snapshotManager = snapshotManager;
        this.schemaManager = schemaManager;
        this.statsFile = statsFile;
        this.sidecarFile = sidecarFile;
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
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();
        if (latestSnapshot == null) {
            throw new IllegalStateException("Unable to obtain the latest snapshot");
        }
        return readStats(latestSnapshot);
    }

    /**
     * Read stats of the specified snapshot.
     *
     * @return stats
     */
    public Optional<Statistics> readStats(Snapshot snapshot) {
        String file = snapshot.statistics();
        return file == null ? Optional.empty() : Optional.of(readStats(file));
    }

    public Statistics readStats(String file) {
        Statistics stats = statsFile.read(file);
        stats.deserializeFieldsFromString(schemaManager.schema(stats.schemaId()));
        return stats;
    }

    public List<StatisticsBlobMetadata> writeSidecar(List<StatisticsBlob> blobs) {
        return sidecarFile().write(blobs);
    }

    public List<StatisticsBlobMetadata> readSidecarMetadata(String fileName) {
        return sidecarFile().readMetadata(fileName);
    }

    public byte[] readSidecar(StatisticsBlobMetadata metadata) {
        return sidecarFile().read(metadata);
    }

    /** Delete stats of the specified snapshot. */
    public void deleteStats(long snapshotId) {
        Snapshot snapshot = snapshotManager.snapshot(snapshotId);
        if (snapshot.statistics() != null) {
            deleteStats(snapshot.statistics());
        }
    }

    public void deleteStats(String statistic) {
        deleteSidecarFiles(statistic);
        statsFile.delete(statistic);
    }

    private void deleteSidecarFiles(String statistic) {
        if (sidecarFile == null) {
            return;
        }

        Statistics stats;
        try {
            stats = statsFile.read(statistic);
        } catch (RuntimeException e) {
            LOG.debug(
                    "Failed to read statistics file {} before deletion. "
                            + "Skipping statistics sidecar file cleanup.",
                    statistic,
                    e);
            return;
        }

        Set<String> sidecarFileNames = new HashSet<>();
        for (StatisticsBlobMetadata metadata : stats.blobMetadata()) {
            sidecarFileNames.add(metadata.fileLocation());
        }
        for (String sidecarFileName : sidecarFileNames) {
            sidecarFile.delete(sidecarFileName);
        }
    }

    private StatisticsSidecarFile sidecarFile() {
        if (sidecarFile == null) {
            throw new IllegalStateException("Statistics sidecar file is not configured.");
        }
        return sidecarFile;
    }
}
