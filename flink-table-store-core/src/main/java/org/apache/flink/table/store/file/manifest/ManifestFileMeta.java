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

import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
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
    private final FieldStats[] partitionStats;

    public ManifestFileMeta(
            String fileName,
            long fileSize,
            long numAddedFiles,
            long numDeletedFiles,
            FieldStats[] partitionStats) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.numAddedFiles = numAddedFiles;
        this.numDeletedFiles = numDeletedFiles;
        this.partitionStats = partitionStats;
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

    public FieldStats[] partitionStats() {
        return partitionStats;
    }

    public static RowType schema(RowType partitionType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_FILE_SIZE", new BigIntType(false)));
        fields.add(new RowType.RowField("_NUM_ADDED_FILES", new BigIntType(false)));
        fields.add(new RowType.RowField("_NUM_DELETED_FILES", new BigIntType(false)));
        fields.add(
                new RowType.RowField(
                        "_PARTITION_STATS", FieldStatsArraySerializer.schema(partitionType)));
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
                && Arrays.equals(partitionStats, that.partitionStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                // by default, hash code of arrays are computed by reference, not by content.
                // so we must use Arrays.hashCode to hash by content.
                Arrays.hashCode(partitionStats));
    }

    @Override
    public String toString() {
        return String.format(
                "{%s, %d, %d, %d, %s}",
                fileName,
                fileSize,
                numAddedFiles,
                numDeletedFiles,
                Arrays.toString(partitionStats));
    }

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same sst file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> metas, ManifestFile manifestFile, long suggestedMetaSize) {
        List<ManifestFileMeta> result = new ArrayList<>();
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();
        List<ManifestFileMeta> candidate = new ArrayList<>();
        long totalSize = 0;

        try {
            for (ManifestFileMeta manifest : metas) {
                totalSize += manifest.fileSize;
                candidate.add(manifest);
                if (totalSize >= suggestedMetaSize) {
                    // reach suggested file size, perform merging and produce new file
                    merge(candidate, manifestFile, result, newMetas);
                    candidate.clear();
                    totalSize = 0;
                }
            }
            if (!candidate.isEmpty()) {
                // merge the last bit of metas
                merge(candidate, manifestFile, result, newMetas);
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

    private static void merge(
            List<ManifestFileMeta> metas,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas) {
        if (metas.size() > 1) {
            ManifestFileMeta newMeta = merge(metas, manifestFile);
            result.add(newMeta);
            newMetas.add(newMeta);
        } else {
            result.addAll(metas);
        }
    }

    private static ManifestFileMeta merge(List<ManifestFileMeta> metas, ManifestFile manifestFile) {
        Preconditions.checkArgument(
                metas.size() > 1, "Number of ManifestFileMeta <= 1. This is a bug.");

        Map<ManifestEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        for (ManifestFileMeta manifest : metas) {
            for (ManifestEntry entry : manifestFile.read(manifest.fileName)) {
                switch (entry.kind()) {
                    case ADD:
                        Preconditions.checkState(
                                !map.containsKey(entry.identifier()),
                                "Trying to add file %s which is already added. Manifest might be corrupted.",
                                entry.identifier());
                        map.put(entry.identifier(), entry);
                        break;
                    case DELETE:
                        // each sst file will only be added once and deleted once,
                        // if we know that it is added before then both add and delete entry can be
                        // removed because there won't be further operations on this file,
                        // otherwise we have to keep the delete entry because the add entry must be
                        // in the previous manifest files
                        if (map.containsKey(entry.identifier())) {
                            map.remove(entry.identifier());
                        } else {
                            map.put(entry.identifier(), entry);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown value kind " + entry.kind().name());
                }
            }
        }
        return manifestFile.write(new ArrayList<>(map.values()));
    }
}
