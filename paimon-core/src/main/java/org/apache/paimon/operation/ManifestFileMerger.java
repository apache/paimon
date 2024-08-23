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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Util for merging manifest files. */
public class ManifestFileMerger {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileMerger.class);

    /**
     * Merge several {@link ManifestFileMeta}s. {@link ManifestEntry}s representing first adding and
     * then deleting the same data file will cancel each other.
     *
     * <p>NOTE: This method is atomic.
     */
    public static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            long manifestFullCompactionSize,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism) {
        // these are the newly created manifest files, clean them up if exception occurs
        List<ManifestFileMeta> newMetas = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newMetas,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newMetas,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount,
                                    manifestReadParallelism));
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newMetas) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism) {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        // merge existing small manifest files
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize();
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                // reach suggested file size, perform merging and produce new file
                mergeCandidates(
                        candidates, manifestFile, result, newMetas, manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(candidates, manifestFile, result, newMetas, manifestReadParallelism);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void mergeCandidates(
            List<ManifestFileMeta> candidates,
            ManifestFile manifestFile,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newMetas,
            @Nullable Integer manifestReadParallelism) {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        Map<FileEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, candidates, map, manifestReadParallelism);
        if (!map.isEmpty()) {
            List<ManifestFileMeta> merged = manifestFile.write(new ArrayList<>(map.values()));
            result.addAll(merged);
            newMetas.addAll(merged);
        }
    }

    public static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newMetas,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        // 1. should trigger full compaction

        List<ManifestFileMeta> base = new ArrayList<>();
        long totalManifestSize = 0;
        int i = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            if (file.numDeletedFiles() == 0 && file.fileSize() >= suggestedMetaSize) {
                base.add(file);
                totalManifestSize += file.fileSize();
            } else {
                break;
            }
        }

        List<ManifestFileMeta> delta = new ArrayList<>();
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (; i < inputs.size(); i++) {
            ManifestFileMeta file = inputs.get(i);
            delta.add(file);
            totalManifestSize += file.fileSize();
            deltaDeleteFileNum += file.numDeletedFiles();
            totalDeltaFileSize += file.fileSize();
        }

        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction

        LOG.info(
                "Start Manifest File Full Compaction, pick the number of delete file: {}, total manifest file size: {}",
                deltaDeleteFileNum,
                totalManifestSize);

        // 2.1. try to skip base files by partition filter

        Map<FileEntry.Identifier, ManifestEntry> deltaMerged = new LinkedHashMap<>();
        FileEntry.mergeEntries(manifestFile, delta, deltaMerged, manifestReadParallelism);

        List<ManifestFileMeta> result = new ArrayList<>();
        int j = 0;
        if (partitionType.getFieldCount() > 0) {
            Set<BinaryRow> deletePartitions = computeDeletePartitions(deltaMerged);
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(partitionType, deletePartitions);
            if (predicate != null) {
                for (; j < base.size(); j++) {
                    // TODO: optimize this to binary search.
                    ManifestFileMeta file = base.get(j);
                    if (predicate.test(
                            file.numAddedFiles() + file.numDeletedFiles(),
                            file.partitionStats().minValues(),
                            file.partitionStats().maxValues(),
                            file.partitionStats().nullCounts())) {
                        break;
                    } else {
                        result.add(file);
                    }
                }
            } else {
                // There is no DELETE Entry in Delta, Base don't need compaction
                j = base.size();
                result.addAll(base);
            }
        }

        // 2.2. try to skip base files by reading entries

        Set<FileEntry.Identifier> deleteEntries = new HashSet<>();
        deltaMerged.forEach(
                (k, v) -> {
                    if (v.kind() == FileKind.DELETE) {
                        deleteEntries.add(k);
                    }
                });

        List<ManifestEntry> mergedEntries = new ArrayList<>();
        for (; j < base.size(); j++) {
            ManifestFileMeta file = base.get(j);
            boolean contains = false;
            for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                checkArgument(entry.kind() == FileKind.ADD);
                if (deleteEntries.contains(entry.identifier())) {
                    contains = true;
                } else {
                    mergedEntries.add(entry);
                }
            }
            if (contains) {
                // already read this file into fullMerged
                j++;
                break;
            } else {
                mergedEntries.clear();
                result.add(file);
            }
        }

        // 2.3. merge

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {

            // 2.3.1 merge mergedEntries
            for (ManifestEntry entry : mergedEntries) {
                writer.write(entry);
            }
            mergedEntries.clear();

            // 2.3.2 merge base files
            for (ManifestEntry entry :
                    FileEntry.readManifestEntries(
                            manifestFile, base.subList(j, base.size()), manifestReadParallelism)) {
                checkArgument(entry.kind() == FileKind.ADD);
                if (!deleteEntries.contains(entry.identifier())) {
                    writer.write(entry);
                }
            }

            // 2.3.3 merge deltaMerged
            for (ManifestEntry entry : deltaMerged.values()) {
                if (entry.kind() == FileKind.ADD) {
                    writer.write(entry);
                }
            }
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                IOUtils.closeQuietly(writer);
                throw exception;
            }
            writer.close();
        }

        List<ManifestFileMeta> merged = writer.result();
        result.addAll(merged);
        newMetas.addAll(merged);
        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(
            Map<FileEntry.Identifier, ManifestEntry> deltaMerged) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (ManifestEntry manifestEntry : deltaMerged.values()) {
            if (manifestEntry.kind() == FileKind.DELETE) {
                BinaryRow partition = manifestEntry.partition();
                partitions.add(partition);
            }
        }
        return partitions;
    }
}
