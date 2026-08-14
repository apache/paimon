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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.avro.AvroBlockWriter;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.PathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Avro writer for manifest entries.
 *
 * <p>The writer accepts materialized entries, encoded records and compressed Avro blocks. It is
 * intentionally separate from the generic writer abstractions because encoded Avro data is an
 * implementation detail of manifest run merging.
 */
public final class ManifestAvroWriter implements AutoCloseable {

    private final FileIO fileIO;
    private final SchemaManager schemaManager;
    private final RowType partitionType;
    private final AvroFileFormat avroFileFormat;
    private final ObjectSerializer<ManifestEntry> serializer;
    private final String compression;
    private final PathFactory pathFactory;
    private final long targetFileSize;

    private final List<ManifestFileMeta> results = new ArrayList<>();
    private final List<Path> completedPaths = new ArrayList<>();
    private @Nullable FileWriter currentWriter;
    private long recordCount;
    private boolean closed;

    ManifestAvroWriter(
            FileIO fileIO,
            SchemaManager schemaManager,
            RowType partitionType,
            AvroFileFormat avroFileFormat,
            ObjectSerializer<ManifestEntry> serializer,
            String compression,
            PathFactory pathFactory,
            long targetFileSize) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.partitionType = partitionType;
        this.avroFileFormat = avroFileFormat;
        this.serializer = serializer;
        this.compression = compression;
        this.pathFactory = pathFactory;
        this.targetFileSize = targetFileSize;
    }

    public void write(ManifestEntry entry) throws IOException {
        try {
            currentWriter().write(entry);
            afterWrite(1, false);
        } catch (IOException | RuntimeException | Error failure) {
            abort();
            throw failure;
        }
    }

    public void write(Iterable<? extends ManifestEntry> entries) throws IOException {
        for (ManifestEntry entry : entries) {
            write(entry);
        }
    }

    public void writeEncoded(ByteBuffer encodedRecord, EncodedEntry metadata) throws IOException {
        try {
            currentWriter().writeEncoded(encodedRecord, metadata);
            afterWrite(1, false);
        } catch (IOException | RuntimeException | Error failure) {
            abort();
            throw failure;
        }
    }

    public void writeEncodedBlock(AvroRawBlock block, EncodedBlock metadata) throws IOException {
        if (metadata.addedFiles < 0 || metadata.deletedFiles < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Manifest block file counts must be non-negative: added %s, deleted %s.",
                            metadata.addedFiles, metadata.deletedFiles));
        }
        long metadataRecordCount = Math.addExact(metadata.addedFiles, metadata.deletedFiles);
        if (metadataRecordCount != block.recordCount()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Manifest block record count mismatch: metadata %s, block %s.",
                            metadataRecordCount, block.recordCount()));
        }
        try {
            currentWriter().writeEncodedBlock(block, metadata);
            afterWrite(metadataRecordCount, true);
        } catch (IOException | RuntimeException | Error failure) {
            abort();
            throw failure;
        }
    }

    private FileWriter currentWriter() {
        if (closed) {
            throw new IllegalStateException("Manifest writer has already closed.");
        }
        if (currentWriter == null) {
            currentWriter = new FileWriter(pathFactory.newPath());
        }
        return currentWriter;
    }

    private void afterWrite(long addedRecords, boolean forceSizeCheck) throws IOException {
        recordCount = Math.addExact(recordCount, addedRecords);
        if (currentWriter.reachTargetSize(
                forceSizeCheck || recordCount % RollingFileWriter.CHECK_ROLLING_RECORD_CNT == 0,
                targetFileSize)) {
            closeCurrentWriter();
        }
    }

    private void closeCurrentWriter() throws IOException {
        if (currentWriter == null) {
            return;
        }
        currentWriter.close();
        ManifestFileMeta result = currentWriter.result();
        completedPaths.add(currentWriter.path);
        results.add(result);
        currentWriter = null;
    }

    public long recordCount() {
        return recordCount;
    }

    public List<ManifestFileMeta> result() {
        if (!closed) {
            throw new IllegalStateException(
                    "Cannot access manifest results before closing the writer.");
        }
        return results;
    }

    public void abort() {
        if (currentWriter != null) {
            currentWriter.abort();
        }
        for (Path path : completedPaths) {
            fileIO.deleteQuietly(path);
        }
        completedPaths.clear();
        results.clear();
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            closeCurrentWriter();
        } catch (IOException | RuntimeException | Error failure) {
            abort();
            throw failure;
        } finally {
            closed = true;
        }
    }

    /** Reusable statistics needed when an encoded manifest record is copied directly. */
    public static final class EncodedEntry {

        private byte kind;
        private BinaryRow partition;
        private int bucket;
        private int level;
        private long schemaId;
        private long firstRowId;
        private long rowCount;

        public EncodedEntry replace(
                byte kind,
                BinaryRow partition,
                int bucket,
                int level,
                long schemaId,
                long firstRowId,
                long rowCount) {
            this.kind = kind;
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.schemaId = schemaId;
            this.firstRowId = firstRowId;
            this.rowCount = rowCount;
            return this;
        }
    }

    /** Aggregate statistics for an encoded Avro block copied without decompression. */
    public static final class EncodedBlock {

        private final long addedFiles;
        private final long deletedFiles;
        private final long schemaId;
        private final int minBucket;
        private final int maxBucket;
        private final int minLevel;
        private final int maxLevel;
        private final long minRowId;
        private final long maxRowId;
        private final @Nullable BinaryRow nullPartition;
        private final long nullPartitionCount;
        private final @Nullable BinaryRow minNonNullPartition;
        private final @Nullable BinaryRow maxNonNullPartition;

        public EncodedBlock(
                long addedFiles,
                long deletedFiles,
                long schemaId,
                int minBucket,
                int maxBucket,
                int minLevel,
                int maxLevel,
                long minRowId,
                long maxRowId,
                @Nullable BinaryRow nullPartition,
                long nullPartitionCount,
                @Nullable BinaryRow minNonNullPartition,
                @Nullable BinaryRow maxNonNullPartition) {
            this.addedFiles = addedFiles;
            this.deletedFiles = deletedFiles;
            this.schemaId = schemaId;
            this.minBucket = minBucket;
            this.maxBucket = maxBucket;
            this.minLevel = minLevel;
            this.maxLevel = maxLevel;
            this.minRowId = minRowId;
            this.maxRowId = maxRowId;
            this.nullPartition = nullPartition;
            this.nullPartitionCount = nullPartitionCount;
            this.minNonNullPartition = minNonNullPartition;
            this.maxNonNullPartition = maxNonNullPartition;
        }
    }

    private final class FileWriter {

        private final Path path;
        private final SimpleStatsCollector partitionStatsCollector;
        private final SimpleStatsConverter partitionStatsSerializer;
        private final Map<BinaryRow, long[]> encodedPartitionCounts = new IdentityHashMap<>();
        private final long[] repeatedNullCounts = new long[partitionType.getFieldCount()];
        private @Nullable PositionOutputStream out;
        private @Nullable AvroBlockWriter writer;
        private @Nullable Long outputBytes;
        private long numAddedFiles;
        private long numDeletedFiles;
        private long schemaId = Long.MIN_VALUE;
        private int minBucket = Integer.MAX_VALUE;
        private int maxBucket = Integer.MIN_VALUE;
        private int minLevel = Integer.MAX_VALUE;
        private int maxLevel = Integer.MIN_VALUE;
        private @Nullable RowIdStats rowIdStats = new RowIdStats();
        private boolean closed;

        private FileWriter(Path path) {
            this.path = path;
            this.partitionStatsCollector = new SimpleStatsCollector(partitionType);
            this.partitionStatsSerializer = new SimpleStatsConverter(partitionType);
            boolean outputCreated = false;
            try {
                out = fileIO.newOutputStream(path, false);
                outputCreated = true;
                writer =
                        avroFileFormat.createBlockWriter(
                                out, ManifestEntry.MANIFEST_ROW_TYPE, compression);
            } catch (IOException failure) {
                IOUtils.closeQuietly(writer);
                IOUtils.closeQuietly(out);
                if (outputCreated) {
                    fileIO.deleteQuietly(path);
                }
                throw new UncheckedIOException(
                        "Failed to create manifest Avro writer for " + path, failure);
            } catch (RuntimeException | Error failure) {
                IOUtils.closeQuietly(writer);
                IOUtils.closeQuietly(out);
                if (outputCreated) {
                    fileIO.deleteQuietly(path);
                }
                throw failure;
            }
        }

        private void write(ManifestEntry entry) throws IOException {
            ensureOpen();
            writer.addElement(
                    entry instanceof ProjectedManifestEntry
                            ? ((ProjectedManifestEntry) entry).fullRow()
                            : serializer.toRow(entry));
            collectStats(entry);
        }

        private void writeEncoded(ByteBuffer encodedRecord, EncodedEntry metadata)
                throws IOException {
            ensureOpen();
            writer.addEncoded(encodedRecord);
            collectStats(metadata);
            addEncodedPartition(metadata.partition, 1);
        }

        private void writeEncodedBlock(AvroRawBlock block, EncodedBlock metadata)
                throws IOException {
            ensureOpen();
            writer.addEncodedBlock(block);
            collectStats(metadata);
            if (metadata.nullPartitionCount > 0) {
                addEncodedPartition(metadata.nullPartition, metadata.nullPartitionCount);
            }
            if (metadata.minNonNullPartition != null) {
                addEncodedPartition(metadata.minNonNullPartition, 1);
                if (metadata.maxNonNullPartition != metadata.minNonNullPartition) {
                    addEncodedPartition(metadata.maxNonNullPartition, 1);
                }
            }
        }

        private void collectStats(ManifestEntry entry) {
            switch (entry.kind()) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind());
            }
            schemaId = Math.max(schemaId, entry.file().schemaId());
            minBucket = Math.min(minBucket, entry.bucket());
            maxBucket = Math.max(maxBucket, entry.bucket());
            minLevel = Math.min(minLevel, entry.level());
            maxLevel = Math.max(maxLevel, entry.level());
            if (rowIdStats != null) {
                Long firstRowId = entry.file().firstRowId();
                if (firstRowId == null) {
                    rowIdStats = null;
                } else {
                    rowIdStats.collect(firstRowId, entry.file().rowCount());
                }
            }
            partitionStatsCollector.collect(entry.partition());
        }

        private void collectStats(EncodedEntry entry) {
            switch (FileKind.fromByteValue(entry.kind)) {
                case ADD:
                    numAddedFiles++;
                    break;
                case DELETE:
                    numDeletedFiles++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown entry kind: " + entry.kind);
            }
            schemaId = Math.max(schemaId, entry.schemaId);
            minBucket = Math.min(minBucket, entry.bucket);
            maxBucket = Math.max(maxBucket, entry.bucket);
            minLevel = Math.min(minLevel, entry.level);
            maxLevel = Math.max(maxLevel, entry.level);
            if (rowIdStats != null) {
                rowIdStats.collect(entry.firstRowId, entry.rowCount);
            }
        }

        private void collectStats(EncodedBlock block) {
            numAddedFiles = Math.addExact(numAddedFiles, block.addedFiles);
            numDeletedFiles = Math.addExact(numDeletedFiles, block.deletedFiles);
            schemaId = Math.max(schemaId, block.schemaId);
            minBucket = Math.min(minBucket, block.minBucket);
            maxBucket = Math.max(maxBucket, block.maxBucket);
            minLevel = Math.min(minLevel, block.minLevel);
            maxLevel = Math.max(maxLevel, block.maxLevel);
            if (rowIdStats != null) {
                rowIdStats.collectRange(block.minRowId, block.maxRowId);
            }
        }

        private void addEncodedPartition(@Nullable BinaryRow partition, long count) {
            if (partition == null || count <= 0) {
                return;
            }
            long[] value =
                    encodedPartitionCounts.computeIfAbsent(partition, ignored -> new long[1]);
            value[0] = Math.addExact(value[0], count);
        }

        private SimpleColStats[] partitionStats() {
            for (Map.Entry<BinaryRow, long[]> entry : encodedPartitionCounts.entrySet()) {
                BinaryRow partition = entry.getKey();
                partitionStatsCollector.collect(partition);
                long repeated = entry.getValue()[0] - 1;
                if (repeated <= 0) {
                    continue;
                }
                for (int field = 0; field < partition.getFieldCount(); field++) {
                    if (partition.isNullAt(field)) {
                        repeatedNullCounts[field] =
                                Math.addExact(repeatedNullCounts[field], repeated);
                    }
                }
            }
            encodedPartitionCounts.clear();
            SimpleColStats[] stats = partitionStatsCollector.extract();
            for (int field = 0; field < stats.length; field++) {
                if (repeatedNullCounts[field] == 0) {
                    continue;
                }
                SimpleColStats current = stats[field];
                stats[field] =
                        new SimpleColStats(
                                current.min(),
                                current.max(),
                                Math.addExact(current.nullCount(), repeatedNullCounts[field]));
            }
            return stats;
        }

        private boolean reachTargetSize(boolean suggestedCheck, long targetSize)
                throws IOException {
            ensureOpen();
            return writer.reachTargetSize(suggestedCheck, targetSize);
        }

        private void ensureOpen() {
            if (closed || writer == null) {
                throw new IllegalStateException("Manifest writer has already closed.");
            }
        }

        private void abort() {
            IOUtils.closeQuietly(writer);
            writer = null;
            IOUtils.closeQuietly(out);
            out = null;
            fileIO.deleteQuietly(path);
            closed = true;
        }

        private void close() throws IOException {
            if (closed) {
                return;
            }
            try {
                writer.close();
                writer = null;
                out.flush();
                outputBytes = out.getPos();
                out.close();
                out = null;
            } catch (IOException | RuntimeException | Error failure) {
                abort();
                throw failure;
            } finally {
                closed = true;
            }
        }

        private ManifestFileMeta result() {
            if (!closed || outputBytes == null) {
                throw new IllegalStateException(
                        "Cannot access manifest result before closing the writer.");
            }
            return new ManifestFileMeta(
                    path.getName(),
                    outputBytes,
                    numAddedFiles,
                    numDeletedFiles,
                    partitionStatsSerializer.toBinaryAllMode(partitionStats()),
                    numAddedFiles + numDeletedFiles > 0
                            ? schemaId
                            : schemaManager.latest().get().id(),
                    minBucket,
                    maxBucket,
                    minLevel,
                    maxLevel,
                    rowIdStats == null ? null : rowIdStats.minRowId,
                    rowIdStats == null ? null : rowIdStats.maxRowId);
        }
    }

    private static class RowIdStats {

        private long minRowId = Long.MAX_VALUE;
        private long maxRowId = Long.MIN_VALUE;

        private void collect(long firstRowId, long rowCount) {
            minRowId = Math.min(minRowId, firstRowId);
            maxRowId = Math.max(maxRowId, firstRowId + rowCount - 1);
        }

        private void collectRange(long minRowId, long maxRowId) {
            this.minRowId = Math.min(this.minRowId, minRowId);
            this.maxRowId = Math.max(this.maxRowId, maxRowId);
        }
    }
}
