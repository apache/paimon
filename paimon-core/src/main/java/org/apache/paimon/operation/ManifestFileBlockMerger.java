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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.io.ProjectedDataFileMeta;
import org.apache.paimon.manifest.CollectedDeletes;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroReader.RawBlock;
import org.apache.paimon.manifest.ManifestAvroReader.RowIterator;
import org.apache.paimon.manifest.ManifestAvroWriter;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionDictionary;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ThreadPoolUtils.CloseableBatchIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecuteCloseable;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Block-aware manifest compaction which never delegates to the legacy full-entry merger. */
final class ManifestFileBlockMerger {

    private static final Logger LOG = LoggerFactory.getLogger(ManifestFileBlockMerger.class);

    private ManifestFileBlockMerger() {}

    static List<ManifestFileMeta> merge(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            CoreOptions options)
            throws Exception {
        long suggestedMetaSize = options.manifestTargetSize().getBytes();
        Integer manifestReadParallelism = options.scanManifestParallelism();
        Optional<List<ManifestFileMeta>> fullCompacted =
                tryFullCompaction(
                        input,
                        newFilesForAbort,
                        manifestFile,
                        suggestedMetaSize,
                        options.manifestFullCompactionThresholdSize().getBytes(),
                        partitionType,
                        manifestReadParallelism);
        if (fullCompacted.isPresent()) {
            return fullCompacted.get();
        }
        return compactMinor(
                input,
                newFilesForAbort,
                manifestFile,
                partitionType,
                suggestedMetaSize,
                options.manifestMergeMinCount(),
                manifestReadParallelism);
    }

    static Optional<List<ManifestFileMeta>> tryFullCompaction(
            List<ManifestFileMeta> inputs,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            long suggestedMetaSize,
            long sizeTrigger,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        checkArgument(sizeTrigger > 0, "Manifest full compaction size trigger cannot be zero.");

        Filter<ManifestFileMeta> mustChange =
                file -> file.numDeletedFiles() > 0 || file.fileSize() < suggestedMetaSize;
        long totalManifestSize = 0;
        long deltaDeleteFileNum = 0;
        long totalDeltaFileSize = 0;
        List<ManifestFileMeta> deltaManifests = new ArrayList<>();
        for (ManifestFileMeta file : inputs) {
            totalManifestSize += file.fileSize();
            if (mustChange.test(file)) {
                totalDeltaFileSize += file.fileSize();
                deltaDeleteFileNum += file.numDeletedFiles();
                deltaManifests.add(file);
            }
        }

        if (totalDeltaFileSize < sizeTrigger) {
            return Optional.empty();
        }

        LOG.info(
                "Start Block-aware Manifest File Full Compaction: totalManifestSize: {}, deltaDeleteFileNum {}, totalDeltaFileSize {}",
                totalManifestSize,
                deltaDeleteFileNum,
                totalDeltaFileSize);

        boolean useRowIdFilter = allContainsRowId(inputs);
        final CollectedDeletes deletes =
                collectDeletes(
                                deltaManifests,
                                manifestFile,
                                useRowIdFilter,
                                true,
                                manifestReadParallelism)
                        .toImmutable();
        try {
            PartitionPredicate predicate;
            if (deletes.isEmpty()) {
                predicate = PartitionPredicate.ALWAYS_FALSE;
            } else if (partitionType.getFieldCount() > 0) {
                predicate = PartitionPredicate.fromMultiple(partitionType, deletes.partitions());
            } else {
                predicate = PartitionPredicate.ALWAYS_TRUE;
            }

            List<ManifestFileMeta> result = new ArrayList<>();
            List<ManifestFileMeta> toCompact = new LinkedList<>(inputs);
            if (predicate != null) {
                Iterator<ManifestFileMeta> iterator = toCompact.iterator();
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

            if (toCompact.size() <= 1) {
                return Optional.empty();
            }

            List<ManifestFileMeta> rewritten =
                    rewriteManifests(
                            toCompact,
                            manifestFile,
                            partitionType,
                            deletes,
                            true,
                            mustChange,
                            result,
                            manifestReadParallelism);
            result.addAll(rewritten);
            newFilesForAbort.addAll(rewritten);
            return Optional.of(result);
        } finally {
            deletes.release();
        }
    }

    private static List<ManifestFileMeta> compactMinor(
            List<ManifestFileMeta> input,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            long suggestedMetaSize,
            int suggestedMinMetaCount,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        List<ManifestFileMeta> result = new ArrayList<>();
        List<ManifestFileMeta> candidates = new ArrayList<>();
        long totalSize = 0;
        for (ManifestFileMeta manifest : input) {
            totalSize += manifest.fileSize();
            candidates.add(manifest);
            if (totalSize >= suggestedMetaSize) {
                compactMinorBatch(
                        candidates,
                        result,
                        newFilesForAbort,
                        manifestFile,
                        partitionType,
                        manifestReadParallelism);
                candidates.clear();
                totalSize = 0;
            }
        }

        if (candidates.size() >= suggestedMinMetaCount) {
            compactMinorBatch(
                    candidates,
                    result,
                    newFilesForAbort,
                    manifestFile,
                    partitionType,
                    manifestReadParallelism);
        } else {
            result.addAll(candidates);
        }
        return result;
    }

    private static void compactMinorBatch(
            List<ManifestFileMeta> candidates,
            List<ManifestFileMeta> result,
            List<ManifestFileMeta> newFilesForAbort,
            ManifestFile manifestFile,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        if (candidates.size() == 1) {
            result.add(candidates.get(0));
            return;
        }

        List<ManifestFileMeta> compacted =
                mergeMinorManifests(
                        candidates, manifestFile, partitionType, manifestReadParallelism);
        result.addAll(compacted);
        newFilesForAbort.addAll(compacted);
    }

    private static CollectedDeletes collectDeletes(
            List<ManifestFileMeta> manifests,
            ManifestFile manifestFile,
            boolean collectRowIds,
            boolean collectPartitions,
            @Nullable Integer manifestReadParallelism) {
        List<ManifestFileMeta> manifestsWithDeletes = new ArrayList<>();
        for (ManifestFileMeta manifest : manifests) {
            if (manifest.numDeletedFiles() > 0) {
                manifestsWithDeletes.add(manifest);
            }
        }

        CollectedDeletes result = new CollectedDeletes(collectRowIds);
        if ((manifestReadParallelism != null && manifestReadParallelism <= 1)
                || manifestsWithDeletes.size() <= 1) {
            for (ManifestFileMeta manifest : manifestsWithDeletes) {
                CollectedDeletes deletes =
                        collectDeletedEntries(
                                manifest, manifestFile, collectRowIds, collectPartitions);
                result.combine(deletes);
                deletes.release();
            }
            return result;
        }

        Function<ManifestFileMeta, List<CollectedDeletes>> scan =
                manifest ->
                        Collections.singletonList(
                                collectDeletedEntries(
                                        manifest, manifestFile, collectRowIds, collectPartitions));
        for (CollectedDeletes deletes :
                sequentialBatchedExecute(scan, manifestsWithDeletes, manifestReadParallelism)) {
            result.combine(deletes);
            deletes.release();
        }
        return result;
    }

    private static CollectedDeletes collectDeletedEntries(
            ManifestFileMeta manifest,
            ManifestFile manifestFile,
            boolean collectRowIds,
            boolean collectPartitions) {
        CollectedDeletes deletes = new CollectedDeletes(collectRowIds);
        try (CloseableIterator<ProjectedManifestEntry> entries =
                manifestFile.scan(
                        manifest.fileName(), ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
            while (entries.hasNext()) {
                ProjectedManifestEntry entry = entries.next();
                if (!entry.isDelete()) {
                    continue;
                }
                deletes.add(entry, collectRowIds, collectPartitions);
            }
            return deletes;
        } catch (Exception e) {
            deletes.release();
            throw new RuntimeException(
                    "Failed to collect DELETE entries from manifest " + manifest.fileName(), e);
        }
    }

    /**
     * Compacts manifests in input order. RowID manifests can copy unaffected ADD-only Avro blocks
     * verbatim; manifests without RowID use identifiers to filter decoded entries.
     */
    private static List<ManifestFileMeta> mergeMinorManifests(
            List<ManifestFileMeta> manifests,
            ManifestFile manifestFile,
            RowType partitionType,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        boolean useRowIdFilter = allContainsRowId(manifests);
        final CollectedDeletes deletes =
                collectDeletes(
                                manifests,
                                manifestFile,
                                useRowIdFilter,
                                false,
                                manifestReadParallelism)
                        .toImmutable();
        try {
            return rewriteManifests(
                    manifests,
                    manifestFile,
                    partitionType,
                    deletes,
                    false,
                    null,
                    null,
                    manifestReadParallelism);
        } finally {
            deletes.release();
        }
    }

    private static List<ManifestFileMeta> rewriteManifests(
            List<ManifestFileMeta> manifests,
            ManifestFile manifestFile,
            RowType partitionType,
            CollectedDeletes deletes,
            boolean fullCompaction,
            @Nullable Filter<ManifestFileMeta> mustChange,
            @Nullable List<ManifestFileMeta> unchangedManifests,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        CompactFileIdentifierSet matchedEntries = new CompactFileIdentifierSet();
        CompactFileIdentifierSet emittedDeletes = new CompactFileIdentifierSet();
        PartitionDictionary partitions = new PartitionDictionary();
        SimpleStatsConverter partitionStatsConverter = new SimpleStatsConverter(partitionType);
        EncodedEntry metadata = new EncodedEntry();
        ReusableIdentifier reusableIdentifier = new ReusableIdentifier();
        boolean hasDeletes = !deletes.isEmpty();
        try {
            // DELETE lookups are immutable, and every planning worker owns its lookup scratch and
            // block statistics. Plan manifests in parallel while the single ordered writer keeps
            // matched ADD and emitted DELETE state on this thread.
            if (hasDeletes
                    && (manifestReadParallelism == null || manifestReadParallelism > 1)
                    && manifests.size() > 1) {
                // Keep decompression and primitive entry inspection parallel. The batched executor
                // bounds retained raw blocks to at most one manifest per planning thread, while the
                // single writer still emits manifests in input order.
                Function<ManifestFileMeta, List<ManifestRewritePlan>> planner =
                        manifest -> {
                            try {
                                return Collections.singletonList(
                                        planManifestRewrite(
                                                manifest, manifestFile, partitionType, deletes));
                            } catch (Exception e) {
                                throw new RuntimeException(
                                        "Failed to plan manifest rewrite for "
                                                + manifest.fileName(),
                                        e);
                            }
                        };
                try (CloseableBatchIterator<ManifestRewritePlan> plans =
                        sequentialBatchedExecuteCloseable(
                                planner, manifests, manifestReadParallelism)) {
                    while (plans.hasNext()) {
                        ManifestRewritePlan plan = plans.next();
                        if (fullCompaction
                                && mustChange != null
                                && !mustChange.test(plan.manifest)
                                && plan.unchanged()) {
                            checkState(
                                    unchangedManifests != null,
                                    "Full compaction requires an unchanged manifest result.");
                            unchangedManifests.add(plan.manifest);
                            continue;
                        }
                        for (PlannedBlock block : plan.blocks) {
                            if (block.compaction.canCopyEncodedBlock()) {
                                writer.writeEncodedBlock(
                                        block.raw.encodedBlock(), block.compaction.metadata);
                            } else {
                                writeBlockEntries(
                                        block.raw,
                                        writer,
                                        deletes,
                                        reusableIdentifier,
                                        fullCompaction,
                                        matchedEntries,
                                        emittedDeletes,
                                        metadata,
                                        plan.encodedRecordsCompatible);
                            }
                        }
                    }
                }
            } else {
                // Otherwise keep the streaming path: add-only manifests already use raw copying,
                // and a single worker or manifest provides no concurrency. Processing one manifest
                // at a time also avoids retaining planned raw blocks unnecessarily.
                for (ManifestFileMeta manifest : manifests) {
                    try (ManifestAvroReader reader =
                            manifestFile.scanAvroBlocks(manifest.fileName(), manifest.fileSize())) {
                        boolean encodedRecordsCompatible = reader.rawBlockCopySupported();
                        if (fullCompaction && mustChange != null && !mustChange.test(manifest)) {
                            boolean rewritten =
                                    rewriteOptionalManifest(
                                            reader,
                                            writer,
                                            partitionType,
                                            partitionStatsConverter,
                                            partitions,
                                            deletes,
                                            reusableIdentifier,
                                            matchedEntries,
                                            emittedDeletes,
                                            metadata,
                                            encodedRecordsCompatible);
                            if (!rewritten) {
                                checkState(
                                        unchangedManifests != null,
                                        "Full compaction requires an unchanged manifest result.");
                                unchangedManifests.add(manifest);
                            }
                            continue;
                        }
                        if (!hasDeletes
                                && manifest.numDeletedFiles() == 0
                                && encodedRecordsCompatible) {
                            writer.writeEncodedManifest(reader, manifest);
                            continue;
                        }
                        writeRemainingBlocks(
                                reader,
                                writer,
                                partitionType,
                                partitionStatsConverter,
                                partitions,
                                deletes,
                                reusableIdentifier,
                                fullCompaction,
                                matchedEntries,
                                emittedDeletes,
                                metadata,
                                encodedRecordsCompatible);
                    } catch (Throwable t) {
                        throw new RuntimeException(
                                "Failed to rewrite manifest file '" + manifest.fileName() + "'.",
                                t);
                    }
                }
            }
            writer.close();
            return writer.result();
        } catch (Exception | Error failure) {
            writer.abort(failure);
            throw failure;
        } finally {
            reusableIdentifier.release();
            matchedEntries.release();
            emittedDeletes.release();
        }
    }

    private static ManifestRewritePlan planManifestRewrite(
            ManifestFileMeta manifest,
            ManifestFile manifestFile,
            RowType partitionType,
            CollectedDeletes deletes)
            throws Exception {
        ReusableIdentifier reusableIdentifier = new ReusableIdentifier();
        PartitionDictionary partitions = new PartitionDictionary();
        SimpleStatsConverter partitionStatsConverter = new SimpleStatsConverter(partitionType);
        try (ManifestAvroReader reader =
                manifestFile.scanAvroBlocks(manifest.fileName(), manifest.fileSize())) {
            boolean encodedRecordsCompatible = reader.rawBlockCopySupported();
            List<PlannedBlock> blocks = new ArrayList<>();
            while (reader.hasNext()) {
                RawBlock raw = reader.next();
                blocks.add(
                        new PlannedBlock(
                                raw.stableCopy(),
                                inspectBlock(
                                        raw,
                                        partitionType,
                                        partitionStatsConverter,
                                        partitions,
                                        deletes,
                                        reusableIdentifier,
                                        encodedRecordsCompatible)));
            }
            return new ManifestRewritePlan(manifest, encodedRecordsCompatible, blocks);
        } finally {
            reusableIdentifier.release();
        }
    }

    private static boolean rewriteOptionalManifest(
            ManifestAvroReader reader,
            ManifestAvroWriter writer,
            RowType partitionType,
            SimpleStatsConverter partitionStatsConverter,
            PartitionDictionary partitions,
            CollectedDeletes deletes,
            ReusableIdentifier reusableIdentifier,
            CompactFileIdentifierSet matchedEntries,
            CompactFileIdentifierSet emittedDeletes,
            EncodedEntry metadata,
            boolean encodedRecordsCompatible)
            throws Exception {
        List<RawBlock> pendingBlocks = new ArrayList<>();
        List<EncodedBlockMeta> pendingMetadata = new ArrayList<>();
        while (reader.hasNext()) {
            RawBlock rawBlock = reader.next();
            CompactionBlock block =
                    inspectBlock(
                            rawBlock,
                            partitionType,
                            partitionStatsConverter,
                            partitions,
                            deletes,
                            reusableIdentifier,
                            encodedRecordsCompatible);
            if (block.unchanged) {
                pendingBlocks.add(rawBlock.stableCopy());
                pendingMetadata.add(block.metadata);
                continue;
            }

            for (int i = 0; i < pendingBlocks.size(); i++) {
                RawBlock pending = pendingBlocks.get(i);
                EncodedBlockMeta blockMetadata = pendingMetadata.get(i);
                if (blockMetadata == null) {
                    writeBlockEntries(
                            pending,
                            writer,
                            deletes,
                            reusableIdentifier,
                            true,
                            matchedEntries,
                            emittedDeletes,
                            metadata,
                            encodedRecordsCompatible);
                } else {
                    writer.writeEncodedBlock(pending.encodedBlock(), blockMetadata);
                }
            }
            writeBlockEntries(
                    rawBlock,
                    writer,
                    deletes,
                    reusableIdentifier,
                    true,
                    matchedEntries,
                    emittedDeletes,
                    metadata,
                    encodedRecordsCompatible);
            writeRemainingBlocks(
                    reader,
                    writer,
                    partitionType,
                    partitionStatsConverter,
                    partitions,
                    deletes,
                    reusableIdentifier,
                    true,
                    matchedEntries,
                    emittedDeletes,
                    metadata,
                    encodedRecordsCompatible);
            return true;
        }
        return false;
    }

    private static void writeRemainingBlocks(
            ManifestAvroReader reader,
            ManifestAvroWriter writer,
            RowType partitionType,
            SimpleStatsConverter partitionStatsConverter,
            PartitionDictionary partitions,
            CollectedDeletes deletes,
            ReusableIdentifier reusableIdentifier,
            boolean fullCompaction,
            CompactFileIdentifierSet matchedEntries,
            CompactFileIdentifierSet emittedDeletes,
            EncodedEntry metadata,
            boolean encodedRecordsCompatible)
            throws Exception {
        while (reader.hasNext()) {
            RawBlock rawBlock = reader.next();
            if (encodedRecordsCompatible) {
                CompactionBlock block =
                        inspectBlock(
                                rawBlock,
                                partitionType,
                                partitionStatsConverter,
                                partitions,
                                deletes,
                                reusableIdentifier,
                                true);
                if (block.canCopyEncodedBlock()) {
                    writer.writeEncodedBlock(rawBlock.encodedBlock(), block.metadata);
                    continue;
                }
            }

            writeBlockEntries(
                    rawBlock,
                    writer,
                    deletes,
                    reusableIdentifier,
                    fullCompaction,
                    matchedEntries,
                    emittedDeletes,
                    metadata,
                    encodedRecordsCompatible);
        }
    }

    private static CompactionBlock inspectBlock(
            RawBlock rawBlock,
            RowType partitionType,
            SimpleStatsConverter partitionStatsConverter,
            PartitionDictionary partitions,
            CollectedDeletes deletes,
            ReusableIdentifier reusableIdentifier,
            boolean encodedRecordsCompatible)
            throws Exception {
        boolean deferDeletedAddCheck = encodedRecordsCompatible && deletes.useRowIdFilter();
        CompactionBlock block = new CompactionBlock(encodedRecordsCompatible, partitionType);
        RowIterator rows =
                rawBlock.toRows(ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.projectedType());
        ProjectedManifestEntry entry = ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.createEntry();
        while (rows.hasNext()) {
            entry.replace(rows.next());
            if (!block.collect(
                    entry, deletes, reusableIdentifier, partitions, deferDeletedAddCheck)) {
                break;
            }
        }
        block.finish(partitionStatsConverter, partitions);
        block.finishFiltering(deletes, deferDeletedAddCheck);
        return block;
    }

    private static void writeBlockEntries(
            RawBlock rawBlock,
            ManifestAvroWriter writer,
            CollectedDeletes deletes,
            ReusableIdentifier reusableIdentifier,
            boolean fullCompaction,
            CompactFileIdentifierSet matchedEntries,
            CompactFileIdentifierSet emittedDeletes,
            EncodedEntry metadata,
            boolean encodedRecordsCompatible)
            throws Exception {
        ProjectedManifestEntry.Projection projection =
                (encodedRecordsCompatible
                        ? ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION
                        : ProjectedManifestEntry.fullProjection());
        RowIterator rows = rawBlock.toRows(projection.projectedType());
        ProjectedManifestEntry entry = projection.createEntry();
        while (rows.hasNext()) {
            GenericRow sourceRow = rows.next();
            entry.replace(sourceRow);
            if (fullCompaction) {
                if (entry.isAdd() && !deletes.isDeleted(entry, reusableIdentifier)) {
                    writeCompactedEntry(
                            writer, rows, sourceRow, entry, metadata, encodedRecordsCompatible);
                }
            } else if (entry.isAdd()) {
                if (deletes.isDeleted(entry, reusableIdentifier)) {
                    matchedEntries.add(reusableIdentifier.replaceWithPartition(entry));
                } else {
                    writeCompactedEntry(
                            writer, rows, sourceRow, entry, metadata, encodedRecordsCompatible);
                }
            } else {
                ReusableIdentifier identifier = reusableIdentifier.replaceWithPartition(entry);
                if (!matchedEntries.contains(identifier) && !emittedDeletes.contains(identifier)) {
                    emittedDeletes.add(identifier);
                    writeCompactedEntry(
                            writer, rows, sourceRow, entry, metadata, encodedRecordsCompatible);
                }
            }
        }
    }

    private static void writeCompactedEntry(
            ManifestAvroWriter writer,
            RowIterator rows,
            GenericRow sourceRow,
            ProjectedManifestEntry entry,
            EncodedEntry metadata,
            boolean encodedRecordsCompatible)
            throws Exception {
        ProjectedDataFileMeta file = entry.file();
        BinaryRow partition = entry.partition();
        if (file.hasFirstRowId()) {
            metadata.replace(
                    entry.kind().toByteValue(),
                    partition,
                    entry.bucket(),
                    file.level(),
                    file.schemaId(),
                    file.nonNullFirstRowId(),
                    file.rowCount());
        } else {
            metadata.replace(
                    entry.kind().toByteValue(),
                    partition,
                    entry.bucket(),
                    file.level(),
                    file.schemaId(),
                    file.rowCount());
        }
        if (encodedRecordsCompatible) {
            writer.writeEncoded(rows.encodedRecord(), metadata);
        } else {
            writer.writeRow(sourceRow, metadata);
        }
    }

    /** Aggregate metadata for one raw Avro block considered by ordinary manifest compaction. */
    private static final class CompactionBlock {

        private boolean unchanged;
        private long addedFiles;
        private long deletedFiles;
        private long schemaId = Long.MIN_VALUE;
        private int minBucket = Integer.MAX_VALUE;
        private int maxBucket = Integer.MIN_VALUE;
        private int minLevel = Integer.MAX_VALUE;
        private int maxLevel = Integer.MIN_VALUE;
        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;
        private boolean hasRowIds = true;
        private final RowType partitionType;
        private final boolean collectMetadata;
        private @Nullable Map<Integer, Integer> partitionCounts;
        private @Nullable EncodedBlockMeta metadata;

        private CompactionBlock(boolean collectMetadata, RowType partitionType) {
            this.unchanged = true;
            this.partitionType = partitionType;
            this.collectMetadata = collectMetadata;
            this.partitionCounts = collectMetadata ? new HashMap<>() : null;
        }

        private boolean collect(
                ProjectedManifestEntry entry,
                CollectedDeletes deletes,
                ReusableIdentifier reusableIdentifier,
                PartitionDictionary partitions,
                boolean deferDeletedAddCheck) {
            if (!unchanged) {
                return false;
            }
            if (!deletes.copyable(entry, reusableIdentifier, deferDeletedAddCheck)) {
                unchanged = false;
                partitionCounts = null;
                return false;
            }
            if (!collectMetadata) {
                return true;
            }

            checkState(partitionCounts != null, "Partition counts have already been released.");
            ProjectedDataFileMeta file = entry.file();
            if (entry.isAdd()) {
                addedFiles++;
            } else {
                deletedFiles++;
            }
            schemaId = Math.max(schemaId, file.schemaId());
            int bucket = entry.bucket();
            minBucket = Math.min(minBucket, bucket);
            maxBucket = Math.max(maxBucket, bucket);
            int level = file.level();
            minLevel = Math.min(minLevel, level);
            maxLevel = Math.max(maxLevel, level);
            if (hasRowIds) {
                if (file.hasFirstRowId()) {
                    long firstRowId = file.nonNullFirstRowId();
                    minRowId = Math.min(minRowId, firstRowId);
                    maxRowId = Math.max(maxRowId, firstRowId + file.rowCount() - 1L);
                } else {
                    hasRowIds = false;
                }
            }
            partitionCounts.merge(partitions.id(entry.partitionBytes()), 1, Integer::sum);
            return true;
        }

        private void finish(
                SimpleStatsConverter partitionStatsConverter, PartitionDictionary partitions) {
            if (!unchanged || !collectMetadata) {
                return;
            }

            checkState(partitionCounts != null, "Partition counts have already been released.");
            SimpleStatsCollector collector = new SimpleStatsCollector(partitionType);
            long[] nullCounts = new long[partitionType.getFieldCount()];
            for (Map.Entry<Integer, Integer> entry : partitionCounts.entrySet()) {
                BinaryRow partition = partitions.partition(entry.getKey());
                collector.collect(partition);
                for (int field = 0; field < nullCounts.length; field++) {
                    if (partition.isNullAt(field)) {
                        nullCounts[field] = Math.addExact(nullCounts[field], entry.getValue());
                    }
                }
            }
            SimpleColStats[] stats = collector.extract();
            for (int field = 0; field < stats.length; field++) {
                stats[field] =
                        new SimpleColStats(
                                stats[field].min(), stats[field].max(), nullCounts[field]);
            }
            metadata =
                    new EncodedBlockMeta(
                            addedFiles,
                            deletedFiles,
                            schemaId,
                            minBucket,
                            maxBucket,
                            minLevel,
                            maxLevel,
                            hasRowIds ? minRowId : -1,
                            hasRowIds ? maxRowId : -1,
                            partitionStatsConverter.toBinaryAllMode(stats));
            partitionCounts = null;
        }

        private void finishFiltering(CollectedDeletes deletes, boolean deferDeletedAddCheck) {
            if (deferDeletedAddCheck && metadata != null) {
                checkState(hasRowIds, "RowID filtering requires block RowID statistics.");
                if (!deletes.intersectsRowIds(minRowId, maxRowId)) {
                    return;
                }
                metadata = null;
                unchanged = false;
            }
        }

        private boolean canCopyEncodedBlock() {
            return metadata != null;
        }
    }

    private static final class PlannedBlock {

        private final RawBlock raw;
        private final CompactionBlock compaction;

        private PlannedBlock(RawBlock raw, CompactionBlock compaction) {
            this.raw = raw;
            this.compaction = compaction;
        }
    }

    private static final class ManifestRewritePlan {

        private final ManifestFileMeta manifest;
        private final boolean encodedRecordsCompatible;
        private final List<PlannedBlock> blocks;

        private ManifestRewritePlan(
                ManifestFileMeta manifest,
                boolean encodedRecordsCompatible,
                List<PlannedBlock> blocks) {
            this.manifest = manifest;
            this.encodedRecordsCompatible = encodedRecordsCompatible;
            this.blocks = blocks;
        }

        private boolean unchanged() {
            for (PlannedBlock block : blocks) {
                if (!block.compaction.unchanged) {
                    return false;
                }
            }
            return true;
        }
    }
}
