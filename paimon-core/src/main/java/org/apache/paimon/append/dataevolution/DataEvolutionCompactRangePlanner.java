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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.ProjectedDataFileMeta;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.manifest.ProjectedManifestEntry.Projection;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.PrimitiveRowRanges;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.LongFunction;

import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.IGNORED_DEDICATED_FILE;
import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.NORMAL_FILE;
import static org.apache.paimon.append.dataevolution.CompactCandidateRangeCollector.VECTOR_FILE;
import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.manifest.ManifestFileMeta.allContainsRowId;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Plans full-metadata scans only for candidates selected from projected live-file metadata. */
final class DataEvolutionCompactRangePlanner {

    // Soft target. One logical candidate range or a legacy manifest group can exceed it.
    static final int CANDIDATE_FILES_PER_BATCH = 100_000;

    private static final Projection CANDIDATE_ADD_PROJECTION =
            manifestProjection(
                    false,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.FILE_SIZE,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.FIRST_ROW_ID);
    private static final Projection CANDIDATE_BLOB_ADD_PROJECTION =
            manifestProjection(
                    false,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.FILE_SIZE,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.SCHEMA_ID,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS);
    private static final Projection CANDIDATE_IDENTIFIER_PROJECTION =
            manifestProjection(
                    true,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.FILE_SIZE,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.LEVEL,
                    DataFileMeta.EXTRA_FILES,
                    DataFileMeta.EMBEDDED_FILE_INDEX,
                    DataFileMeta.EXTERNAL_PATH,
                    DataFileMeta.FIRST_ROW_ID);
    private static final Projection CANDIDATE_BLOB_IDENTIFIER_PROJECTION =
            manifestProjection(
                    true,
                    DataFileMeta.FILE_NAME,
                    DataFileMeta.FILE_SIZE,
                    DataFileMeta.ROW_COUNT,
                    DataFileMeta.SCHEMA_ID,
                    DataFileMeta.LEVEL,
                    DataFileMeta.EXTRA_FILES,
                    DataFileMeta.EMBEDDED_FILE_INDEX,
                    DataFileMeta.EXTERNAL_PATH,
                    DataFileMeta.FIRST_ROW_ID,
                    DataFileMeta.WRITE_COLS);

    private final ManifestFile manifestFile;
    private final @Nullable PartitionPredicate partitionPredicate;
    private final int candidateFilesPerBatch;
    private final CandidateOptions candidateOptions;

    DataEvolutionCompactRangePlanner(
            ManifestFile manifestFile,
            @Nullable PartitionPredicate partitionPredicate,
            int candidateFilesPerBatch,
            CandidateOptions candidateOptions) {
        this.manifestFile = manifestFile;
        this.partitionPredicate = partitionPredicate;
        checkArgument(candidateFilesPerBatch > 0, "Candidate files per batch must be positive.");
        this.candidateFilesPerBatch = candidateFilesPerBatch;
        this.candidateOptions = Objects.requireNonNull(candidateOptions, "candidateOptions");
    }

    Queue<List<ManifestFileMeta>> groupManifestFiles(List<ManifestFileMeta> manifestFileMetas) {
        if (!allContainsRowId(manifestFileMetas)) {
            // File-level first row id is still available in projected manifest entries. Keep all
            // manifests in one compatibility group so candidates can span manifests even when
            // older manifest metadata does not contain row-id bounds.
            return new ArrayDeque<>(Collections.singletonList(manifestFileMetas));
        }

        RangeHelper<ManifestFileMeta> rangeHelper =
                new RangeHelper<>(
                        manifest ->
                                new Range(
                                        manifest.minRowId(),
                                        manifest.maxRowId() < Long.MAX_VALUE
                                                ? manifest.maxRowId() + 1L
                                                : manifest.maxRowId()));
        return new ArrayDeque<>(rangeHelper.mergeOverlappingRanges(manifestFileMetas));
    }

    Iterator<RangeBatch> plan(List<ManifestFileMeta> manifestFileMetas) {
        return new RangeBatchIterator(groupManifestFiles(manifestFileMetas));
    }

    private Queue<RangeBatch> planManifestGroup(List<ManifestFileMeta> manifestGroup) {
        RangeBatchBuilder batches = new RangeBatchBuilder(candidateFilesPerBatch, manifestGroup);
        CompactFileIdentifierSet deletedIdentifiers = new CompactFileIdentifierSet();
        ReusableIdentifier identifier = new ReusableIdentifier();
        CompactCandidateRangeCollector candidateRanges =
                new CompactCandidateRangeCollector(
                        initialCandidateCapacity(manifestGroup),
                        candidateOptions.targetFileSize,
                        candidateOptions.blobTargetFileSize,
                        candidateOptions.openFileCost,
                        candidateOptions.compactMinFileNum);
        try {
            collectDeletedIdentifiers(manifestGroup, deletedIdentifiers, identifier);
            collectCandidateRanges(manifestGroup, deletedIdentifiers, identifier, candidateRanges);
            identifier.release();
            deletedIdentifiers.release();
            candidateRanges.finish(batches::add);
            return batches.finish();
        } catch (RuntimeException | Error e) {
            identifier.release();
            deletedIdentifiers.release();
            candidateRanges.abort();
            throw e;
        }
    }

    private void collectDeletedIdentifiers(
            List<ManifestFileMeta> manifestGroup,
            CompactFileIdentifierSet deletedIdentifiers,
            ReusableIdentifier identifier) {
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numDeletedFiles() <= 0) {
                continue;
            }
            try (CloseableIterator<ProjectedManifestEntry> entries =
                    manifestFile.scan(
                            manifestMeta.fileName(),
                            ProjectedManifestEntry.DELETE_ENTRY_PROJECTION)) {
                while (entries.hasNext()) {
                    ProjectedManifestEntry entry = entries.next();
                    if (entry.isDelete() && includePartition(entry)) {
                        deletedIdentifiers.add(identifier.replaceWithPartition(entry));
                    }
                }
            } catch (Exception e) {
                throw scanException(manifestMeta, e);
            }
        }
    }

    private void collectCandidateRanges(
            List<ManifestFileMeta> manifestGroup,
            CompactFileIdentifierSet deletedIdentifiers,
            ReusableIdentifier identifier,
            CompactCandidateRangeCollector candidateRanges) {
        Projection projection = candidateProjection(!deletedIdentifiers.isEmpty());
        Map<BinaryRow, Integer> partitionIds = new HashMap<>();
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            if (manifestMeta.numAddedFiles() <= 0) {
                continue;
            }
            try (CloseableIterator<ProjectedManifestEntry> entries =
                    manifestFile.scan(manifestMeta.fileName(), projection)) {
                while (entries.hasNext()) {
                    ProjectedManifestEntry entry = entries.next();
                    if (!entry.isAdd()) {
                        continue;
                    }
                    BinaryRow partition = entry.partition();
                    if (partitionPredicate != null && !partitionPredicate.test(partition)) {
                        continue;
                    }
                    if (!deletedIdentifiers.isEmpty()
                            && deletedIdentifiers.contains(
                                    identifier.replaceWithPartition(entry))) {
                        continue;
                    }

                    ProjectedDataFileMeta file = entry.file();
                    checkState(
                            file.hasFirstRowId(),
                            "File '%s' does not have first row id.",
                            file.fileNameBinary());
                    long rowCount = file.rowCount();
                    checkState(
                            rowCount > 0,
                            "File '%s' has non-positive row count %s.",
                            file.fileNameBinary(),
                            rowCount);
                    Integer partitionId =
                            partitionIds.computeIfAbsent(partition, k -> partitionIds.size());
                    candidateRanges.add(
                            partitionId,
                            candidateFileKind(file),
                            file.nonNullFirstRowId(),
                            rowCount,
                            file.fileSize());
                }
            } catch (Exception e) {
                throw scanException(manifestMeta, e);
            }
        }
    }

    private Projection candidateProjection(boolean includeIdentifier) {
        if (candidateOptions.compactBlob) {
            return includeIdentifier
                    ? CANDIDATE_BLOB_IDENTIFIER_PROJECTION
                    : CANDIDATE_BLOB_ADD_PROJECTION;
        }
        return includeIdentifier ? CANDIDATE_IDENTIFIER_PROJECTION : CANDIDATE_ADD_PROJECTION;
    }

    private int candidateFileKind(ProjectedDataFileMeta file) {
        String fileName = file.fileNameBinary().toString();
        if (isBlobFile(fileName)) {
            return candidateOptions.compactBlob
                    ? candidateOptions.blobFieldId(file)
                    : IGNORED_DEDICATED_FILE;
        }
        if (isVectorStoreFile(fileName)) {
            return candidateOptions.compactVector ? VECTOR_FILE : IGNORED_DEDICATED_FILE;
        }
        return NORMAL_FILE;
    }

    private boolean includePartition(ProjectedManifestEntry entry) {
        return partitionPredicate == null || partitionPredicate.test(entry.partition());
    }

    private int initialCandidateCapacity(List<ManifestFileMeta> manifestGroup) {
        long addedFiles = 0L;
        long deletedFiles = 0L;
        for (ManifestFileMeta manifestMeta : manifestGroup) {
            addedFiles = Math.addExact(addedFiles, manifestMeta.numAddedFiles());
            deletedFiles = Math.addExact(deletedFiles, manifestMeta.numDeletedFiles());
        }
        long estimatedLiveFiles = Math.max(0L, addedFiles - deletedFiles);
        return (int) Math.min(estimatedLiveFiles, candidateFilesPerBatch);
    }

    private static Projection manifestProjection(
            boolean includeBucket, String... projectedFileFields) {
        List<DataField> fields = new ArrayList<>();
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.KIND));
        fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.PARTITION));
        if (includeBucket) {
            fields.add(ManifestEntry.MANIFEST_ROW_TYPE.getField(ManifestEntry.BUCKET));
        }
        fields.add(
                ManifestEntry.MANIFEST_ROW_TYPE
                        .getField(ManifestEntry.FILE)
                        .newType(DataFileMeta.SCHEMA.project(projectedFileFields)));
        return Projection.create(new RowType(false, fields));
    }

    private static RuntimeException scanException(ManifestFileMeta manifestMeta, Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new RuntimeException("Failed to scan manifest file " + manifestMeta.fileName(), e);
    }

    static final class RangeBatch {

        private final long[] starts;
        private final long[] ends;
        private final List<ManifestFileMeta> manifestFiles;
        private final int fileCount;

        private RangeBatch(
                PrimitiveRowRanges.Owned ranges,
                List<ManifestFileMeta> manifestFiles,
                int fileCount) {
            this(ranges.starts(), ranges.ends(), manifestFiles, fileCount);
        }

        private RangeBatch(
                long[] starts, long[] ends, List<ManifestFileMeta> manifestFiles, int fileCount) {
            this.starts = starts;
            this.ends = ends;
            this.manifestFiles = manifestFiles;
            this.fileCount = fileCount;
        }

        private RangeBatch merge(RangeBatch other) {
            long[] mergedStarts = new long[starts.length + other.starts.length];
            long[] mergedEnds = new long[ends.length + other.ends.length];
            System.arraycopy(starts, 0, mergedStarts, 0, starts.length);
            System.arraycopy(other.starts, 0, mergedStarts, starts.length, other.starts.length);
            System.arraycopy(ends, 0, mergedEnds, 0, ends.length);
            System.arraycopy(other.ends, 0, mergedEnds, ends.length, other.ends.length);
            List<ManifestFileMeta> mergedManifestFiles =
                    new ArrayList<>(manifestFiles.size() + other.manifestFiles.size());
            mergedManifestFiles.addAll(manifestFiles);
            mergedManifestFiles.addAll(other.manifestFiles);
            return new RangeBatch(
                    mergedStarts,
                    mergedEnds,
                    mergedManifestFiles,
                    Math.addExact(fileCount, other.fileCount));
        }

        List<Range> toRanges() {
            List<Range> ranges = new ArrayList<>(starts.length);
            for (int i = 0; i < starts.length; i++) {
                ranges.add(new Range(starts[i], ends[i]));
            }
            return ranges;
        }

        List<ManifestFileMeta> manifestFiles() {
            return manifestFiles;
        }

        int fileCount() {
            return fileCount;
        }
    }

    private static final class RangeBatchBuilder {

        private final int candidateFilesPerBatch;
        private final List<ManifestFileMeta> manifestFiles;
        private final boolean manifestsHaveRowIdBounds;
        private final Queue<RangeBatch> batches = new ArrayDeque<>();
        private PrimitiveRowRanges current = new PrimitiveRowRanges(16);
        private long currentFileCount;

        private RangeBatchBuilder(
                int candidateFilesPerBatch, List<ManifestFileMeta> manifestFiles) {
            this.manifestsHaveRowIdBounds = allContainsRowId(manifestFiles);
            // Legacy manifests cannot be pruned by row-id bounds. Keep their candidate ranges in
            // one batch so the full manifest group is scanned only once.
            this.candidateFilesPerBatch =
                    manifestsHaveRowIdBounds ? candidateFilesPerBatch : Integer.MAX_VALUE;
            this.manifestFiles = manifestFiles;
        }

        private void add(long start, long end, int fileCount) {
            checkArgument(fileCount > 0, "Logical range file count must be positive.");
            if (current.size() > 0 && currentFileCount + fileCount > candidateFilesPerBatch) {
                flush();
            }
            current.add(start, end);
            currentFileCount = Math.addExact(currentFileCount, fileCount);
            if (currentFileCount >= candidateFilesPerBatch) {
                flush();
            }
        }

        private Queue<RangeBatch> finish() {
            flush();
            return batches;
        }

        private void flush() {
            if (current.size() == 0) {
                return;
            }
            List<ManifestFileMeta> batchManifestFiles = manifestsForCurrentRanges();
            batches.add(
                    new RangeBatch(
                            current.takeOwned(),
                            batchManifestFiles,
                            Math.toIntExact(currentFileCount)));
            current = new PrimitiveRowRanges(16);
            currentFileCount = 0L;
        }

        private List<ManifestFileMeta> manifestsForCurrentRanges() {
            if (!manifestsHaveRowIdBounds) {
                return manifestFiles;
            }
            List<ManifestFileMeta> overlapping = new ArrayList<>();
            for (ManifestFileMeta manifestFile : manifestFiles) {
                if (current.overlaps(manifestFile.minRowId(), manifestFile.maxRowId())) {
                    overlapping.add(manifestFile);
                }
            }
            checkState(!overlapping.isEmpty(), "Candidate ranges must overlap a manifest file.");
            return overlapping;
        }
    }

    private final class RangeBatchIterator implements Iterator<RangeBatch> {

        private final Queue<List<ManifestFileMeta>> manifestGroups;
        private Queue<RangeBatch> currentGroupBatches = new ArrayDeque<>();
        @Nullable private RangeBatch pending;

        private RangeBatchIterator(Queue<List<ManifestFileMeta>> manifestGroups) {
            this.manifestGroups = manifestGroups;
        }

        @Override
        public boolean hasNext() {
            ensurePending();
            return pending != null;
        }

        @Override
        public RangeBatch next() {
            ensurePending();
            if (pending == null) {
                throw new NoSuchElementException();
            }

            RangeBatch result = pending;
            pending = null;
            while (result.fileCount() < candidateFilesPerBatch) {
                ensurePending();
                if (pending == null
                        || (long) result.fileCount() + pending.fileCount()
                                > candidateFilesPerBatch) {
                    break;
                }
                result = result.merge(pending);
                pending = null;
            }
            return result;
        }

        private void ensurePending() {
            while (pending == null) {
                if (!currentGroupBatches.isEmpty()) {
                    pending = currentGroupBatches.poll();
                    return;
                }
                List<ManifestFileMeta> manifestGroup = manifestGroups.poll();
                if (manifestGroup == null) {
                    return;
                }
                currentGroupBatches = planManifestGroup(manifestGroup);
            }
        }
    }

    static final class CandidateOptions {

        private final boolean compactBlob;
        private final boolean compactVector;
        private final long targetFileSize;
        private final long blobTargetFileSize;
        private final long openFileCost;
        private final long compactMinFileNum;
        private final LongFunction<RowType> schemaFetcher;
        private final @Nullable Set<Integer> currentBlobFieldIds;

        CandidateOptions(
                boolean compactBlob,
                boolean compactVector,
                long targetFileSize,
                long blobTargetFileSize,
                long openFileCost,
                long compactMinFileNum,
                LongFunction<RowType> schemaFetcher,
                @Nullable Set<Integer> currentBlobFieldIds) {
            this.compactBlob = compactBlob;
            this.compactVector = compactVector;
            this.targetFileSize = targetFileSize;
            this.blobTargetFileSize = blobTargetFileSize;
            this.openFileCost = openFileCost;
            this.compactMinFileNum = compactMinFileNum;
            Map<Long, RowType> schemaCache = new HashMap<>();
            this.schemaFetcher =
                    schemaId -> schemaCache.computeIfAbsent(schemaId, schemaFetcher::apply);
            this.currentBlobFieldIds = currentBlobFieldIds;
        }

        private int blobFieldId(ProjectedDataFileMeta blobFile) {
            List<String> writeCols = blobFile.writeCols();
            checkArgument(
                    writeCols != null && writeCols.size() == 1,
                    "Blob file %s should contain exactly one write column.",
                    blobFile.fileNameBinary());
            int fieldId = schemaFetcher.apply(blobFile.schemaId()).getField(writeCols.get(0)).id();
            checkArgument(fieldId >= 0, "Blob field id cannot be negative.");
            return currentBlobFieldIds == null || currentBlobFieldIds.contains(fieldId)
                    ? fieldId
                    : IGNORED_DEDICATED_FILE;
        }
    }
}
