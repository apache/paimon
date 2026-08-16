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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;

/** Manifest file merger with standard merge logic and optional sort rewrite. */
public class ManifestFileMerger {

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options) {
        return merge(input, manifestFile, partitionType, options, null);
    }

    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options,
            @Nullable IOManager ioManager) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();

        try {
            // If manifest-sort.enabled is enabled and there are sortable fields, use
            // trySortRewrite. Data evolution tables sort by RowID when all manifest files contain
            // RowID ranges, so they do not require partition fields.
            if (canUseManifestSort(input, partitionType, options)) {
                return ManifestFileSorter.trySortCompaction(
                        input, newFilesForAbort, manifestFile, partitionType, options, ioManager);
            }

            if (options.manifestMergeOptimizeEnabled()) {
                return ManifestFileBlockMerger.merge(
                        input, newFilesForAbort, manifestFile, partitionType, options);
            }
            return ManifestFileLegacyMerger.merge(
                    input, newFilesForAbort, manifestFile, partitionType, options);
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newFilesForAbort) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    static boolean canUseManifestSort(
            List<ManifestFileMeta> input, RowType partitionType, CoreOptions options) {
        return options.manifestSortEnabled()
                && (partitionType.getFieldCount() > 0
                        || (options.dataEvolutionEnabled() && allContainsRowId(input)));
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        return ManifestFileBlockMerger.tryFullCompaction(
                inputs,
                newFilesForAbort,
                manifestFile,
                suggestedMetaSize,
                sizeTrigger,
                partitionType,
                manifestReadParallelism);
    }
}
