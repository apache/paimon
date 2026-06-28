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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.ManifestFileMerger;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;

/** Compact manifest file to reduce deleted manifest entries. */
public class CompactManifestProcedure extends ProcedureBase {

    private static final String COMMIT_USER = "Compact-Manifest-Procedure-Committer";

    @Override
    public String identifier() {
        return "compact_manifest";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "options", type = @DataTypeHint("STRING"), isOptional = true),
                @ArgumentHint(name = "dry_run", type = @DataTypeHint("BOOLEAN"), isOptional = true)
            })
    public String[] call(
            ProcedureContext procedureContext,
            String tableId,
            @Nullable String options,
            @Nullable Boolean dryRun)
            throws Exception {

        FileStoreTable table = (FileStoreTable) table(tableId);
        HashMap<String, String> dynamicOptions = new HashMap<>();
        ProcedureUtils.putIfNotEmpty(
                dynamicOptions, CoreOptions.COMMIT_USER_PREFIX.key(), COMMIT_USER);
        ProcedureUtils.putAllOptions(dynamicOptions, options);

        table = table.copy(dynamicOptions);

        if (dryRun != null && dryRun) {
            return dryRunCompactManifest(table);
        }

        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.compactManifests();
        }
        return new String[] {"success"};
    }

    private String[] dryRunCompactManifest(FileStoreTable table) {
        Snapshot latestSnapshot = table.store().snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            return new String[] {"Dry run: no snapshot exists, nothing to compact."};
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> beforeManifests = manifestList.readDataManifests(latestSnapshot);

        Options compactOptions = Options.fromMap(table.options());
        compactOptions.set(CoreOptions.MANIFEST_MERGE_MIN_COUNT, 1);
        compactOptions.set(CoreOptions.MANIFEST_FULL_COMPACTION_FILE_SIZE, MemorySize.ofBytes(1));

        List<ManifestFileMeta> afterManifests =
                ManifestFileMerger.merge(
                        beforeManifests,
                        table.store().manifestFileFactory().create(),
                        table.schema().logicalPartitionType(),
                        new CoreOptions(compactOptions),
                        null);

        long beforeFileCount = beforeManifests.size();
        long afterFileCount = afterManifests.size();
        long beforeTotalSize = beforeManifests.stream().mapToLong(ManifestFileMeta::fileSize).sum();
        long afterTotalSize = afterManifests.stream().mapToLong(ManifestFileMeta::fileSize).sum();
        long beforeDeletedEntries =
                beforeManifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum();
        long afterDeletedEntries =
                afterManifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum();
        long eliminatedDeletedEntries = beforeDeletedEntries - afterDeletedEntries;

        String result =
                String.format(
                        "Dry run: manifest compaction would reduce %d manifest files to %d, "
                                + "total file size from %s to %s, "
                                + "eliminating %d deleted entries.",
                        beforeFileCount,
                        afterFileCount,
                        MemorySize.ofBytes(beforeTotalSize),
                        MemorySize.ofBytes(afterTotalSize),
                        eliminatedDeletedEntries);
        return new String[] {result};
    }
}
