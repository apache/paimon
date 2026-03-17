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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.globalindex.GlobalIndexCompactor;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compactor for BTree global index that delegates to {@link BTreeGlobalIndexCompactCoordinator} for
 * planning and {@link BTreeGlobalIndexCompactWorker} for execution.
 *
 * <p>This class maintains backward compatibility with the {@link GlobalIndexCompactor} interface
 * while internally using the coordinator-worker pattern for compaction.
 */
public class BTreeGlobalIndexCompactor implements GlobalIndexCompactor {

    private final BTreeGlobalIndexCompactCoordinator coordinator;
    private final BTreeGlobalIndexCompactWorker worker;

    public BTreeGlobalIndexCompactor(
            FileIO fileIO, RowType rowType, Options options, IndexPathFactory indexPathFactory) {
        this.coordinator = new BTreeGlobalIndexCompactCoordinator(options);
        this.worker = new BTreeGlobalIndexCompactWorker(fileIO, rowType, options, indexPathFactory);
    }

    /**
     * Compacts all mergeable groups from input index manifest entries.
     *
     * <p>Returns empty list if no group meets the min-files threshold. Otherwise returns unchanged
     * entries plus newly compacted entries.
     */
    @Override
    public List<IndexManifestEntry> compact(List<IndexManifestEntry> entries) throws IOException {
        if (!coordinator.meetsMinFilesThreshold(entries)) {
            return new ArrayList<>(entries);
        }

        List<BTreeGlobalIndexCompactTask> tasks = coordinator.plan(entries);
        if (tasks.isEmpty()) {
            // Min-files met but no mergeable segments (e.g. gap ranges)
            return new ArrayList<>(entries);
        }

        // Collect source file names that will be compacted
        Set<String> compactedFileNames = new HashSet<>();
        for (BTreeGlobalIndexCompactTask task : tasks) {
            for (IndexManifestEntry e : task.sourceEntries()) {
                compactedFileNames.add(e.indexFile().fileName());
            }
        }

        // Pass-through entries not involved in compaction
        List<IndexManifestEntry> result = new ArrayList<>();
        for (IndexManifestEntry entry : entries) {
            if (!compactedFileNames.contains(entry.indexFile().fileName())) {
                result.add(entry);
            }
        }

        // Execute tasks and collect compacted results
        for (BTreeGlobalIndexCompactTask task : tasks) {
            result.addAll(worker.execute(task));
        }
        return result;
    }
}
