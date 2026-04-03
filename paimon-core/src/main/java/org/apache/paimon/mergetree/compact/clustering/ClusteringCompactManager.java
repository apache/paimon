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

package org.apache.paimon.mergetree.compact.clustering;

import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compact.CompactDeletionFile;
import org.apache.paimon.compact.CompactFutureManager;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.lookup.sort.db.SimpleLsmKvDb;
import org.apache.paimon.operation.metrics.CompactionMetrics;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;

/**
 * Key Value clustering compact manager for {@link KeyValueFileStore}.
 *
 * <p>Compaction is triggered when unsorted files exist. The compaction process has two phases:
 *
 * <ol>
 *   <li><b>Phase 1</b>: Sort and rewrite all unsorted (level 0) files by clustering columns.
 *   <li><b>Phase 2</b>: Merge sorted files based on clustering column key range overlap. Files are
 *       grouped into sections where each section contains overlapping files. Adjacent sections are
 *       merged when beneficial (overlapping files or small sections) to reduce IO amplification
 *       while consolidating small files.
 * </ol>
 */
public class ClusteringCompactManager extends CompactFutureManager {

    private final RowType keyType;
    private final RowType valueType;
    private final ExecutorService executor;
    private final BucketedDvMaintainer dvMaintainer;
    private final boolean lazyGenDeletionFile;
    @Nullable private final CompactionMetrics.Reporter metricsReporter;

    private final ClusteringFiles fileLevels;
    private final ClusteringKeyIndex keyIndex;
    private final ClusteringFileRewriter fileRewriter;

    public ClusteringCompactManager(
            RowType keyType,
            RowType valueType,
            List<String> clusteringColumns,
            IOManager ioManager,
            CacheManager cacheManager,
            KeyValueFileReaderFactory keyReaderFactory,
            KeyValueFileReaderFactory valueReaderFactory,
            KeyValueFileWriterFactory writerFactory,
            ExecutorService executor,
            BucketedDvMaintainer dvMaintainer,
            boolean lazyGenDeletionFile,
            List<DataFileMeta> restoreFiles,
            long targetFileSize,
            long sortSpillBufferSize,
            int pageSize,
            int maxNumFileHandles,
            int spillThreshold,
            CompressOptions compression,
            boolean firstRow,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.executor = executor;
        this.dvMaintainer = dvMaintainer;
        this.lazyGenDeletionFile = lazyGenDeletionFile;
        this.metricsReporter = metricsReporter;

        this.fileLevels = new ClusteringFiles();
        restoreFiles.forEach(this::addNewFile);

        int[] clusteringColumnIndexes = valueType.projectIndexes(clusteringColumns);
        RecordComparator clusteringComparatorAlone =
                CodeGenUtils.newRecordComparator(
                        valueType.project(clusteringColumns).getFieldTypes(),
                        IntStream.range(0, clusteringColumns.size()).toArray(),
                        true);
        RecordComparator clusteringComparatorInValue =
                CodeGenUtils.newRecordComparator(
                        valueType.getFieldTypes(), clusteringColumnIndexes, true);

        SimpleLsmKvDb kvDb =
                SimpleLsmKvDb.builder(new File(ioManager.pickTempDir()))
                        .cacheManager(cacheManager)
                        .keyComparator(new RowCompactedSerializer(keyType).createSliceComparator())
                        .build();

        this.keyIndex =
                new ClusteringKeyIndex(
                        keyType,
                        ioManager,
                        keyReaderFactory,
                        dvMaintainer,
                        kvDb,
                        fileLevels,
                        firstRow,
                        sortSpillBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        compression);
        keyIndex.bootstrap(restoreFiles);

        this.fileRewriter =
                new ClusteringFileRewriter(
                        keyType,
                        valueType,
                        clusteringColumnIndexes,
                        clusteringComparatorAlone,
                        clusteringComparatorInValue,
                        ioManager,
                        valueReaderFactory,
                        writerFactory,
                        fileLevels,
                        targetFileSize,
                        sortSpillBufferSize,
                        pageSize,
                        maxNumFileHandles,
                        spillThreshold,
                        compression);
    }

    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    @Override
    public void addNewFile(DataFileMeta file) {
        fileLevels.addNewFile(file);
    }

    @Override
    public List<DataFileMeta> allFiles() {
        return fileLevels.allFiles();
    }

    @Override
    public void triggerCompaction(boolean fullCompaction) {
        if (taskFuture != null) {
            return;
        }
        taskFuture =
                executor.submit(
                        new CompactTask(metricsReporter) {
                            @Override
                            protected CompactResult doCompact() throws Exception {
                                return compact(fullCompaction);
                            }
                        });
    }

    private CompactResult compact(boolean fullCompaction) throws Exception {
        KeyValueSerializer kvSerializer = new KeyValueSerializer(keyType, valueType);
        RowType kvSchemaType = KeyValue.schema(keyType, valueType);

        CompactResult result = new CompactResult();

        // Phase 1: Sort and rewrite all unsorted (level 0) files
        List<DataFileMeta> unsortedFiles = fileLevels.unsortedFiles();
        // Snapshot sorted files before Phase 1 to avoid including newly created files in Phase 2
        List<DataFileMeta> existingSortedFiles = fileLevels.sortedFiles();
        for (DataFileMeta file : unsortedFiles) {
            List<DataFileMeta> sortedFiles =
                    fileRewriter.sortAndRewriteFiles(
                            singletonList(file), kvSerializer, kvSchemaType);
            keyIndex.updateIndex(file, sortedFiles);
            result.before().add(file);
            result.after().addAll(sortedFiles);
        }

        // Phase 2: Universal Compaction on sorted files that existed before Phase 1.
        List<List<DataFileMeta>> mergeGroups;
        if (fullCompaction) {
            mergeGroups = singletonList(existingSortedFiles);
        } else {
            mergeGroups = fileRewriter.pickMergeCandidates(existingSortedFiles);
        }

        for (List<DataFileMeta> mergeGroup : mergeGroups) {
            if (mergeGroup.size() >= 2) {
                // Delete key index entries before merge
                for (DataFileMeta file : mergeGroup) {
                    keyIndex.deleteIndex(file);
                }
                List<DataFileMeta> mergedFiles = fileRewriter.mergeAndRewriteFiles(mergeGroup);
                // Rebuild key index for new files
                for (DataFileMeta newFile : mergedFiles) {
                    keyIndex.rebuildIndex(newFile);
                }
                // Remove stale deletion vectors for merged-away files
                for (DataFileMeta file : mergeGroup) {
                    dvMaintainer.removeDeletionVectorOf(file.fileName());
                }
                result.before().addAll(mergeGroup);
                result.after().addAll(mergedFiles);
            }
        }

        CompactDeletionFile deletionFile =
                lazyGenDeletionFile
                        ? CompactDeletionFile.lazyGeneration(dvMaintainer)
                        : CompactDeletionFile.generateFiles(dvMaintainer);
        result.setDeletionFile(deletionFile);
        return result;
    }

    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        return innerGetCompactionResult(blocking);
    }

    @Override
    public boolean compactNotCompleted() {
        return super.compactNotCompleted() || fileLevels.compactNotCompleted();
    }

    @Override
    public void close() throws IOException {
        keyIndex.close();
    }
}
