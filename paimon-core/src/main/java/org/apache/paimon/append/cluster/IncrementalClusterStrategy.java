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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.schema.SchemaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/** Cluster strategy to decide which files to select for cluster. */
public class IncrementalClusterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalClusterStrategy.class);

    private final List<String> clusterKeys;
    private final SchemaManager schemaManager;

    private final UniversalCompaction universalCompaction;

    public IncrementalClusterStrategy(
            SchemaManager schemaManager,
            List<String> clusterKeys,
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger) {
        this.universalCompaction =
                new UniversalCompaction(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null, null);
        this.clusterKeys = clusterKeys;
        this.schemaManager = schemaManager;
    }

    public Optional<CompactUnit> pick(
            int numLevels, List<LevelSortedRun> runs, boolean fullCompaction) {
        if (fullCompaction) {
            return pickFullCompaction(numLevels, runs);
        }
        return universalCompaction.pick(numLevels, runs);
    }

    public Optional<CompactUnit> pickFullCompaction(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            if (LOG.isDebugEnabled()) {
                LOG.debug("no sorted run, no need to compact");
            }
            return Optional.empty();
        }

        if (runs.size() == 1 && runs.get(0).level() == maxLevel) {
            long schemaId = runs.get(0).run().files().get(0).schemaId();
            CoreOptions coreOptions = CoreOptions.fromMap(schemaManager.schema(schemaId).options());
            // only one sorted run in the maxLevel with the same cluster key
            if (coreOptions.clusteringColumns().equals(clusterKeys)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "only one sorted run in the maxLevel with the same cluster key, no need to compact");
                }
                return Optional.empty();
            }
        }

        // full compaction
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}
