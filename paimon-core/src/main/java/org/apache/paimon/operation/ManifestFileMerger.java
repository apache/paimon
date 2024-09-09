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
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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
        List<ManifestFileMeta> newFilesForAbort = new ArrayList<>();

        try {
            Optional<List<ManifestFileMeta>> fullCompacted =
                    tryFullCompaction(
                            input,
                            newFilesForAbort,
                            manifestFile,
                            suggestedMetaSize,
                            manifestFullCompactionSize,
                            partitionType,
                            manifestReadParallelism);
            return fullCompacted.orElseGet(
                    () ->
                            tryMinorCompaction(
                                    input,
                                    newFilesForAbort,
                                    manifestFile,
                                    suggestedMetaSize,
                                    suggestedMinMetaCount,
                                    manifestReadParallelism));
        } catch (Throwable e) {
            // exception occurs, clean up and rethrow
            for (ManifestFileMeta manifest : newFilesForAbort) {
                manifestFile.delete(manifest.fileName());
            }
            throw new RuntimeException(e);
        }
    }

    private static List<ManifestFileMeta> tryMinorCompaction(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
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
                        candidates,
                        manifestFile,
                        result,
                        newFilesForAbort,
                        manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        // merge the last bit of manifests if there are too many
        if (candidates.size() >= suggestedMinMetaCount) {
            mergeCandidates(
                    candidates, manifestFile, result, newFilesForAbort, manifestReadParallelism);
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
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        checkArgument(sizeTrigger > 0, "Manifest full compaction size trigger cannot be zero.");

        // 1. should trigger full compaction

        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
        long totalManifestSize = 0;
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        for (ManifestFileMeta file : inputs) {
            totalManifestSize += file.fileSize();
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
                deltaDeleteFileNum += file.numDeletedFiles();
            }
        }

        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        // 2. do full compaction

        LOG.info(
                "Start Manifest File Full Compaction: totalManifestSize: {}, deltaDeleteFileNum {}, totalDeltaFileSize {}",
                totalManifestSize,
                deltaDeleteFileNum,
                totalDeltaFileSize);

        // 2.1. read all delete entries

        Set<FileEntry.Identifier> deleteEntries =
                FileEntry.readDeletedEntries(manifestFile, inputs, manifestReadParallelism);

        // 2.2. try to skip base files by partition filter

        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> toBeMerged = new LinkedList<>(inputs);
        if (partitionType.getFieldCount() > 0) {
            Set<BinaryRow> deletePartitions = computeDeletePartitions(deleteEntries);
            PartitionPredicate predicate =
                    PartitionPredicate.fromMultiple(partitionType, deletePartitions);
            if (predicate != null) {
                Iterator<ManifestFileMeta> iterator = toBeMerged.iterator();
                while (iterator.hasNext()) {
                    ManifestFileMeta file = iterator.next();
                    if (mustChange.test(file)) {
                        continue;
                    }
                    if (!predicate.test(
                            file.numAddedFiles() + file.numDeletedFiles(),
                            file.partitionStats().minValues(),
                            file.partitionStats().maxValues(),
                            file.partitionStats().nullCounts())) {
                        iterator.remove();
                        result.add(file);
                    }
                }
            }
        }

        // 2.2. merge

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {
            for (ManifestFileMeta file : toBeMerged) {
                List<ManifestEntry> entries = new ArrayList<>();
                boolean requireChange = mustChange.test(file);
                for (ManifestEntry entry : manifestFile.read(file.fileName(), file.fileSize())) {
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    }

                    if (deleteEntries.contains(entry.identifier())) {
                        requireChange = true;
                    } else {
                        entries.add(entry);
                    }
                }

                if (requireChange) {
                    writer.write(entries);
                } else {
                    result.add(file);
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
        newFilesForAbort.addAll(merged);
        return Optional.of(result);
    }

    private static Set<BinaryRow> computeDeletePartitions(Set<FileEntry.Identifier> deleteEntries) {
        Set<BinaryRow> partitions = new HashSet<>();
        for (FileEntry.Identifier identifier : deleteEntries) {
            partitions.add(identifier.partition);
        }
        return partitions;
    }
}
