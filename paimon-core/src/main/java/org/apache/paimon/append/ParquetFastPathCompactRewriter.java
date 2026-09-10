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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.parquet.ParquetInputFile;
import org.apache.paimon.format.parquet.ParquetRowGroupCopier;
import org.apache.paimon.format.parquet.ParquetRowGroupCopyChecker;
import org.apache.paimon.format.parquet.ParquetRowGroupCopyChecker.Incompatibility;
import org.apache.paimon.format.parquet.ParquetSimpleStatsExtractor;
import org.apache.paimon.format.parquet.ParquetUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics;
import org.apache.paimon.operation.metrics.CompactionFastPathMetrics.MissReason;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.stats.SimpleStatsMerger;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.paimon.shade.org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.paimon.CoreOptions.FILE_FORMAT_PARQUET;
import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.ExceptionUtils.stripExecutionException;
import static org.apache.paimon.utils.StatsCollectorFactories.createStatsFactories;

/** Fast-path rewriter that concatenates Parquet RowGroups for append-only compaction. */
public class ParquetFastPathCompactRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFastPathCompactRewriter.class);

    private ParquetFastPathCompactRewriter() {}

    @Nullable
    public static List<DataFileMeta> tryRewrite(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType writeType,
            CoreOptions options,
            BinaryRow partition,
            int bucket,
            @Nullable Function<String, DeletionVector> dvFactory,
            List<DataFileMeta> toCompact,
            DataFilePathFactory pathFactory,
            long schemaId,
            @Nullable CompactionFastPathMetrics metrics,
            @Nullable ParquetFooterReadExecutor footerReadExecutor) {
        long startNanos = System.nanoTime();
        long inputBytes = toCompact.stream().mapToLong(DataFileMeta::fileSize).sum();
        long inputRows = toCompact.stream().mapToLong(DataFileMeta::rowCount).sum();
        try {
            if (dvFactory != null) {
                reportMiss(
                        metrics,
                        MissReason.DV,
                        String.format(
                                "inputFiles=%d, inputRows=%d, inputBytes=%d",
                                toCompact.size(), inputRows, inputBytes));
                return null;
            }

            ParquetRowGroupCopyChecker checker =
                    new ParquetRowGroupCopyChecker(
                            writeType, options.fileCompression(), options.toConfiguration());
            List<String> expectedValueStatsCols = toCompact.get(0).valueStatsCols();

            MissReason tableMiss =
                    checkTableLevel(
                            options,
                            fileFormat,
                            bucket,
                            expectedValueStatsCols,
                            toCompact,
                            checker);
            if (tableMiss != null) {
                reportMiss(
                        metrics,
                        tableMiss,
                        String.format(
                                "bucket=%d, fileFormat=%s, inputFiles=%d",
                                bucket, options.fileFormatString(), toCompact.size()));
                return null;
            }

            List<FooterRequest> footerRequests = new ArrayList<>(toCompact.size());
            for (int i = 0; i < toCompact.size(); i++) {
                DataFileMeta file = toCompact.get(i);
                MissReason fileMiss = checkFileLevel(file, schemaId, expectedValueStatsCols);
                if (fileMiss != null) {
                    reportMiss(
                            metrics,
                            fileMiss,
                            String.format(
                                    "file=%s, fileIndex=%d, schemaId=%d",
                                    file.fileName(), i, file.schemaId()));
                    return null;
                }
                Path filePath = pathFactory.toPath(file);
                footerRequests.add(
                        new FooterRequest(
                                i,
                                file,
                                ParquetInputFile.fromPath(fileIO, filePath, file.fileSize()),
                                filePath));
            }

            FooterReadStats footerReadStats =
                    readFooters(fileIO, options, footerRequests, footerReadExecutor);
            List<PreparedInput> preparedInputs = new ArrayList<>(footerRequests.size());
            for (FooterRequest request : footerRequests) {
                IndexedFooter indexedFooter = footerReadStats.orderedFooters[request.fileIndex];
                Incompatibility incompatibility = checker.checkFooter(indexedFooter.footer);
                if (incompatibility != null) {
                    reportMiss(
                            metrics,
                            toMissReason(incompatibility),
                            String.format(
                                    "file=%s, fileIndex=%d, expectedCodec=%s",
                                    request.file.fileName(),
                                    request.fileIndex,
                                    checker.expectedCodec()));
                    return null;
                }
                preparedInputs.add(
                        new PreparedInput(request.file, request.inputFile, indexedFooter.footer));
            }

            long prepareMs = elapsedMillis(startNanos);
            long footerReadWallMs = nanosToMillis(footerReadStats.footerReadWallNanos);
            long footerReadSumMs = nanosToMillis(footerReadStats.footerReadSumNanos);
            int footerConcurrency = footerReadStats.footerConcurrency;
            int inputRowGroups =
                    preparedInputs.stream().mapToInt(input -> input.blocks.size()).sum();
            List<ParquetRowGroupCopier.Input> copierInputs = new ArrayList<>(preparedInputs.size());
            for (PreparedInput preparedInput : preparedInputs) {
                copierInputs.add(
                        new ParquetRowGroupCopier.Input(
                                preparedInput.inputFile, preparedInput.metadata));
            }

            ParquetRowGroupCopier copier =
                    new ParquetRowGroupCopier(
                            fileIO,
                            writeType,
                            options.targetFileSize(false),
                            pathFactory::newPath,
                            options.toConfiguration(),
                            options.appendCompactionRowGroupCopyPreservePageIndex());
            long copyStartNanos = System.nanoTime();
            List<ParquetRowGroupCopier.OutputFile> copiedFiles = copier.copy(copierInputs);
            long copyMs = elapsedMillis(copyStartNanos);
            try {
                long buildStartNanos = System.nanoTime();
                List<DataFileMeta> result =
                        buildResult(
                                copiedFiles,
                                preparedInputs,
                                toCompact,
                                writeType,
                                schemaId,
                                expectedValueStatsCols,
                                options,
                                pathFactory);
                long buildResultMs = elapsedMillis(buildStartNanos);
                long outputBytes = result.stream().mapToLong(DataFileMeta::fileSize).sum();
                long outputRows = result.stream().mapToLong(DataFileMeta::rowCount).sum();
                if (metrics != null) {
                    metrics.reportHit();
                }
                LOG.info(
                        "Append compaction fast path succeeded: inputFiles={}, inputRows={}, "
                                + "inputBytes={}, inputRowGroups={}, outputFiles={}, outputRows={}, "
                                + "outputBytes={}, prepareMs={}, footerReadWallMs={}, "
                                + "footerReadSumMs={}, footerConcurrency={}, copyMs={}, "
                                + "buildResultMs={}, totalMs={}, preservePageIndex={}",
                        toCompact.size(),
                        inputRows,
                        inputBytes,
                        inputRowGroups,
                        result.size(),
                        outputRows,
                        outputBytes,
                        prepareMs,
                        footerReadWallMs,
                        footerReadSumMs,
                        footerConcurrency,
                        copyMs,
                        buildResultMs,
                        elapsedMillis(startNanos),
                        options.appendCompactionRowGroupCopyPreservePageIndex());
                return result;
            } catch (IOException | RuntimeException e) {
                cleanupCopiedFiles(fileIO, copiedFiles);
                throw e;
            }
        } catch (IOException e) {
            reportMiss(
                    metrics,
                    MissReason.IO_ERROR,
                    String.format(
                            "inputFiles=%d, inputRows=%d, inputBytes=%d, error=%s",
                            toCompact.size(), inputRows, inputBytes, e.toString()));
            LOG.info("Append compaction fast path failed with IO error, fallback to rewrite", e);
            return null;
        } catch (RuntimeException e) {
            reportMiss(
                    metrics,
                    MissReason.IO_ERROR,
                    String.format(
                            "inputFiles=%d, inputRows=%d, inputBytes=%d, error=%s",
                            toCompact.size(), inputRows, inputBytes, e.toString()));
            LOG.info("Append compaction fast path failed, fallback to rewrite", e);
            return null;
        }
    }

    private static List<DataFileMeta> buildResult(
            List<ParquetRowGroupCopier.OutputFile> copiedFiles,
            List<PreparedInput> preparedInputs,
            List<DataFileMeta> toCompact,
            RowType writeType,
            long schemaId,
            @Nullable List<String> valueStatsCols,
            CoreOptions options,
            DataFilePathFactory pathFactory)
            throws IOException {
        boolean isExternalPath = pathFactory.isExternalPath();
        LongCounter sequenceCounter = new LongCounter(toCompact.get(0).minSequenceNumber());
        List<DataFileMeta> result = new ArrayList<>(copiedFiles.size());
        for (ParquetRowGroupCopier.OutputFile copiedFile : copiedFiles) {
            SimpleStats valueStats =
                    mergeOutputValueStats(
                            copiedFile.blockContributions(),
                            preparedInputs,
                            writeType,
                            valueStatsCols,
                            options);

            long minSequenceNumber = sequenceCounter.getValue();
            sequenceCounter.add(copiedFile.rowCount());
            long maxSequenceNumber = sequenceCounter.getValue() - 1;

            String externalPath = isExternalPath ? copiedFile.path().toString() : null;
            result.add(
                    DataFileMeta.forAppend(
                            copiedFile.path().getName(),
                            copiedFile.fileSize(),
                            copiedFile.rowCount(),
                            valueStats,
                            minSequenceNumber,
                            maxSequenceNumber,
                            schemaId,
                            Collections.emptyList(),
                            null,
                            FileSource.COMPACT,
                            valueStatsCols,
                            externalPath,
                            null,
                            null));
        }

        long inputRowCount = toCompact.stream().mapToLong(DataFileMeta::rowCount).sum();
        long outputRowCount = result.stream().mapToLong(DataFileMeta::rowCount).sum();
        if (inputRowCount != outputRowCount) {
            throw new IOException(
                    String.format(
                            "Row count mismatch after RowGroup copy: input %d but output %d",
                            inputRowCount, outputRowCount));
        }
        return result;
    }

    private static SimpleStats mergeOutputValueStats(
            List<ParquetRowGroupCopier.BlockContribution> contributions,
            List<PreparedInput> preparedInputs,
            RowType writeType,
            @Nullable List<String> valueStatsCols,
            CoreOptions options)
            throws IOException {
        List<SimpleStats> statsToMerge = new ArrayList<>();
        List<BlockMetaData> partialBlocks = new ArrayList<>();
        Map<Integer, List<Integer>> blockIndicesByFile = new LinkedHashMap<>();
        for (ParquetRowGroupCopier.BlockContribution contribution : contributions) {
            blockIndicesByFile
                    .computeIfAbsent(contribution.fileIndex(), ignored -> new ArrayList<>())
                    .add(contribution.blockIndex());
        }

        for (Map.Entry<Integer, List<Integer>> entry : blockIndicesByFile.entrySet()) {
            int fileIndex = entry.getKey();
            PreparedInput preparedInput = preparedInputs.get(fileIndex);
            List<Integer> blockIndices = entry.getValue();
            if (isFullFile(blockIndices, preparedInput.blocks.size())) {
                statsToMerge.add(preparedInput.file.valueStats());
            } else {
                for (int blockIndex : blockIndices) {
                    partialBlocks.add(preparedInput.blocks.get(blockIndex));
                }
            }
        }

        if (!partialBlocks.isEmpty()) {
            statsToMerge.add(statsFromBlocks(partialBlocks, writeType, valueStatsCols, options));
        }
        return SimpleStatsMerger.merge(statsToMerge, writeType, valueStatsCols);
    }

    private static boolean isFullFile(List<Integer> blockIndices, int totalBlocks) {
        if (blockIndices.size() != totalBlocks) {
            return false;
        }
        for (int i = 0; i < totalBlocks; i++) {
            if (blockIndices.get(i) != i) {
                return false;
            }
        }
        return true;
    }

    private static SimpleStats statsFromBlocks(
            List<BlockMetaData> blocks,
            RowType writeType,
            @Nullable List<String> valueStatsCols,
            CoreOptions options)
            throws IOException {
        SimpleColStatsCollector.Factory[] collectors =
                createStatsFactories(options.statsMode(), options, writeType.getFieldNames());
        ParquetSimpleStatsExtractor extractor =
                new ParquetSimpleStatsExtractor(options.toConfiguration(), writeType, collectors);
        SimpleColStats[] colStats = extractor.extractFromBlocks(blocks);
        Pair<List<String>, SimpleStats> converted =
                new SimpleStatsConverter(writeType, options.statsDenseStore()).toBinary(colStats);
        if (!SimpleStatsMerger.sameValueStatsCols(valueStatsCols, converted.getLeft())) {
            throw new IOException(
                    String.format(
                            "Partial block stats columns mismatch: expected %s but got %s",
                            valueStatsCols, converted.getLeft()));
        }
        return converted.getRight();
    }

    private static void cleanupCopiedFiles(
            FileIO fileIO, List<ParquetRowGroupCopier.OutputFile> copiedFiles) {
        for (ParquetRowGroupCopier.OutputFile copiedFile : copiedFiles) {
            fileIO.deleteQuietly(copiedFile.path());
        }
    }

    @Nullable
    private static MissReason checkTableLevel(
            CoreOptions options,
            FileFormat fileFormat,
            int bucket,
            @Nullable List<String> expectedValueStatsCols,
            List<DataFileMeta> toCompact,
            ParquetRowGroupCopyChecker checker) {
        if (bucket != UNAWARE_BUCKET) {
            return MissReason.OTHER;
        }
        if (!FILE_FORMAT_PARQUET.equals(options.fileFormatString())) {
            return MissReason.OTHER;
        }
        if (!FILE_FORMAT_PARQUET.equals(fileFormat.getFormatIdentifier())) {
            return MissReason.OTHER;
        }
        if (options.rowTrackingEnabled() || options.dataEvolutionEnabled()) {
            return MissReason.ROW_TRACKING;
        }
        Incompatibility configurationMiss = checker.checkConfiguration();
        if (configurationMiss != null) {
            return toMissReason(configurationMiss);
        }
        if (!options.indexColumnsOptions().isEmpty()) {
            return MissReason.FILE_INDEX;
        }
        for (DataFileMeta file : toCompact) {
            if (!SimpleStatsMerger.sameValueStatsCols(
                    expectedValueStatsCols, file.valueStatsCols())) {
                return MissReason.OTHER;
            }
        }
        return null;
    }

    @Nullable
    private static MissReason checkFileLevel(
            DataFileMeta file, long schemaId, @Nullable List<String> expectedValueStatsCols) {
        if (file.schemaId() != schemaId) {
            return MissReason.SCHEMA_ID;
        }
        if (!file.extraFiles().isEmpty()) {
            return MissReason.EXTRA_FILES;
        }
        if (file.embeddedIndex() != null) {
            return MissReason.EXTRA_FILES;
        }
        if (file.firstRowId() != null) {
            return MissReason.ROW_TRACKING;
        }
        if (file.writeCols() != null) {
            return MissReason.WRITE_COLS;
        }
        if (file.fileSource().isPresent()) {
            FileSource fileSource = file.fileSource().get();
            if (fileSource != FileSource.APPEND && fileSource != FileSource.COMPACT) {
                return MissReason.FILE_SOURCE;
            }
        }
        if (!SimpleStatsMerger.sameValueStatsCols(expectedValueStatsCols, file.valueStatsCols())) {
            return MissReason.OTHER;
        }
        return null;
    }

    private static MissReason toMissReason(Incompatibility incompatibility) {
        switch (incompatibility) {
            case ENCRYPTION:
                return MissReason.ENCRYPTION;
            case SCHEMA_MISMATCH:
                return MissReason.MESSAGE_TYPE;
            case CODEC_MISMATCH:
                return MissReason.CODEC;
            case BLOOM_FILTER:
                return MissReason.BLOOM_CONFIGURED;
            default:
                return MissReason.OTHER;
        }
    }

    private static FooterReadStats readFooters(
            FileIO fileIO,
            CoreOptions options,
            List<FooterRequest> requests,
            @Nullable ParquetFooterReadExecutor footerReadExecutor)
            throws IOException {
        int parallelism =
                footerReadExecutor == null
                        ? 1
                        : footerReadExecutor.effectiveParallelism(requests.size());
        if (parallelism <= 1) {
            return readFootersSerially(fileIO, options, requests);
        }
        return readFootersConcurrently(
                fileIO, options, requests, footerReadExecutor.executor(), parallelism);
    }

    private static FooterReadStats readFootersSerially(
            FileIO fileIO, CoreOptions options, List<FooterRequest> requests) throws IOException {
        long wallStartNanos = System.nanoTime();
        long footerReadSumNanos = 0L;
        IndexedFooter[] ordered = new IndexedFooter[requests.size()];
        for (FooterRequest request : requests) {
            long taskStartNanos = System.nanoTime();
            try {
                ordered[request.fileIndex] = readFooter(request, fileIO, options);
            } finally {
                footerReadSumNanos += System.nanoTime() - taskStartNanos;
            }
        }
        return new FooterReadStats(
                ordered, System.nanoTime() - wallStartNanos, footerReadSumNanos, 1);
    }

    private static FooterReadStats readFootersConcurrently(
            FileIO fileIO,
            CoreOptions options,
            List<FooterRequest> requests,
            ExecutorService executor,
            int parallelism)
            throws IOException {
        long wallStartNanos = System.nanoTime();
        AtomicLong footerReadSumNanos = new AtomicLong();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        Semaphore semaphore = new Semaphore(parallelism);
        List<Future<IndexedFooter>> futures = new ArrayList<>(requests.size());
        for (FooterRequest request : requests) {
            futures.add(
                    executor.submit(
                            () -> {
                                try {
                                    semaphore.acquire();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException(
                                            "Interrupted while reading Parquet footers", e);
                                }
                                int current = active.incrementAndGet();
                                updateMax(maxActive, current);
                                long taskStartNanos = System.nanoTime();
                                try {
                                    return readFooter(request, fileIO, options);
                                } finally {
                                    footerReadSumNanos.addAndGet(
                                            System.nanoTime() - taskStartNanos);
                                    active.decrementAndGet();
                                    semaphore.release();
                                }
                            }));
        }

        IndexedFooter[] ordered = new IndexedFooter[requests.size()];
        try {
            for (Future<IndexedFooter> future : futures) {
                IndexedFooter result = future.get();
                ordered[result.fileIndex] = result;
            }
        } catch (InterruptedException e) {
            cancelAll(futures);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading Parquet footers", e);
        } catch (ExecutionException e) {
            cancelAll(futures);
            throw unwrapIOException(e);
        }
        return new FooterReadStats(
                ordered,
                System.nanoTime() - wallStartNanos,
                footerReadSumNanos.get(),
                Math.max(1, maxActive.get()));
    }

    private static IndexedFooter readFooter(
            FooterRequest request, FileIO fileIO, CoreOptions options) throws IOException {
        return new IndexedFooter(
                request.fileIndex,
                ParquetUtil.readFooter(
                        fileIO,
                        request.filePath,
                        request.file.fileSize(),
                        options.toConfiguration()));
    }

    private static void updateMax(AtomicInteger maxActive, int current) {
        int observed;
        do {
            observed = maxActive.get();
            if (current <= observed) {
                return;
            }
        } while (!maxActive.compareAndSet(observed, current));
    }

    private static void cancelAll(List<Future<IndexedFooter>> futures) {
        for (Future<IndexedFooter> future : futures) {
            future.cancel(true);
        }
    }

    private static IOException unwrapIOException(ExecutionException e) {
        Throwable cause = stripExecutionException(e);
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        return new IOException("Failed to read Parquet footers", cause);
    }

    private static void reportMiss(
            @Nullable CompactionFastPathMetrics metrics, MissReason reason, String detail) {
        if (metrics != null) {
            metrics.reportMiss(reason);
        }
        LOG.info("Append compaction fast path miss: reason={}, {}", reason, detail);
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    private static long nanosToMillis(long nanos) {
        return nanos / 1_000_000;
    }

    private static final class PreparedInput {
        private final DataFileMeta file;
        private final ParquetInputFile inputFile;
        private final ParquetMetadata metadata;
        private final List<BlockMetaData> blocks;

        private PreparedInput(
                DataFileMeta file, ParquetInputFile inputFile, ParquetMetadata metadata) {
            this.file = file;
            this.inputFile = inputFile;
            this.metadata = metadata;
            this.blocks = metadata.getBlocks();
        }
    }

    private static final class FooterRequest {
        private final int fileIndex;
        private final DataFileMeta file;
        private final ParquetInputFile inputFile;
        private final Path filePath;

        private FooterRequest(
                int fileIndex, DataFileMeta file, ParquetInputFile inputFile, Path filePath) {
            this.fileIndex = fileIndex;
            this.file = file;
            this.inputFile = inputFile;
            this.filePath = filePath;
        }
    }

    private static final class IndexedFooter {
        private final int fileIndex;
        private final ParquetMetadata footer;

        private IndexedFooter(int fileIndex, ParquetMetadata footer) {
            this.fileIndex = fileIndex;
            this.footer = footer;
        }
    }

    private static final class FooterReadStats {
        private final IndexedFooter[] orderedFooters;
        private final long footerReadWallNanos;
        private final long footerReadSumNanos;
        private final int footerConcurrency;

        private FooterReadStats(
                IndexedFooter[] orderedFooters,
                long footerReadWallNanos,
                long footerReadSumNanos,
                int footerConcurrency) {
            this.orderedFooters = orderedFooters;
            this.footerReadWallNanos = footerReadWallNanos;
            this.footerReadSumNanos = footerReadSumNanos;
            this.footerConcurrency = footerConcurrency;
        }
    }
}
