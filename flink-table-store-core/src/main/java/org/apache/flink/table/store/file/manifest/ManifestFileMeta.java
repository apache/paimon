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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.store.file.stats.BinaryTableStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Metadata of a manifest file. */
public class ManifestFileMeta {

    private final String fileName;
    private final long fileSize;
    private final long numAddedFiles;
    private final long numDeletedFiles;
    private final BinaryTableStats partitionStats;
    private final long schemaId;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            BinaryTableStats partitionStats,
            long schemaId) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
        this.schemaId = schemaId;
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long numAddedFiles() {
        return numAddedFiles;
    }

    public long numDeletedFiles() {
        return numDeletedFiles;
    }

    public BinaryTableStats partitionStats() {
        return partitionStats;
    }

    public long schemaId() {
        return schemaId;
    }

    public static RowType schema() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_FILE_SIZE", new BigIntType(false)));
        fields.add(new RowType.RowField("_NUM_ADDED_FILES", new BigIntType(false)));
        fields.add(new RowType.RowField("_NUM_DELETED_FILES", new BigIntType(false)));
        fields.add(new RowType.RowField("_PARTITION_STATS", FieldStatsArraySerializer.schema()));
        fields.add(new RowType.RowField("_SCHEMA_ID", new BigIntType(false)));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestFileMeta)) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && numAddedFiles == that.numAddedFiles
                && numDeletedFiles == that.numDeletedFiles
                && Objects.equals(partitionStats, that.partitionStats)
                && schemaId == that.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s, %d}",
                fileName, fileSize, numAddedFiles, numDeletedFiles, partitionStats, schemaId);
    }

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> metas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount) {
        List<ManifestFileMeta> result = new ArrayList<>();
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;

        try {
            // merge existing manifests first
            for (ManifestFileMeta manifest : metas) {
                totalSize += manifest.fileSize;
                candidates.add(manifest);
                if (totalSize >= suggestedMetaSize) {
                    // reach suggested file size, perform merging and produce new file
                    mergeCandidates(candidates, manifestFile, result, newMetas);
                    candidates.clear();
                    totalSize = 0;
                }
            }

            // merge the last bit of manifests if there are too many
            if (candidates.size() >= suggestedMinMetaCount) {
                mergeCandidates(candidates, manifestFile, result, newMetas);
            } else {
                result.addAll(candidates);
            }
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName);
            }
            throw e;
        }

        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas) {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        Map<ManifestEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        for (ManifestFileMeta manifest : candidates) {
            ManifestEntry.mergeEntries(manifestFile.read(manifest.fileName), map);
        }
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }
}
