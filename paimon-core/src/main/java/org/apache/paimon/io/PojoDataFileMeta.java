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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** A {@link DataFileMeta} using pojo objects. */
public class PojoDataFileMeta implements DataFileMeta {

    private final String fileName;
    private final long fileSize;

    // total number of rows (including add & delete) in this file
    private final long rowCount;

    private final BinaryRow minKey;
    private final BinaryRow maxKey;
    private final SimpleStats keyStats;
    private final SimpleStats valueStats;

    // As for row-tracking table, this will be reassigned while committing
    private final long minSequenceNumber;
    private final long maxSequenceNumber;
    private final long schemaId;
    private final int level;

    private final List<String> extraFiles;
    private final Timestamp creationTime;

    // rowCount = addRowCount + deleteRowCount
    // Why don't we keep addRowCount and deleteRowCount?
    // Because in previous versions of DataFileMeta, we only keep rowCount.
    // We have to keep the compatibility.
    private final @Nullable Long deleteRowCount;

    // file index filter bytes, if it is small, store in data file meta
    private final @Nullable byte[] embeddedIndex;

    private final @Nullable FileSource fileSource;

    private final @Nullable List<String> valueStatsCols;

    /** external path of file, if it is null, it is in the default warehouse path. */
    private final @Nullable String externalPath;

    private final @Nullable Long firstRowId;

    private final @Nullable List<String> writeCols;

    /**
     * Commit snapshot id stamped on this file. Non-null only when the table has {@code
     * sequence.snapshot-ordering} enabled and the file has been assigned a snapshot id either at
     * commit time or by a compaction rewriter. Null for files written before the feature was
     * enabled or for tables that do not use snapshot-based ordering.
     */
    private final @Nullable Long commitSnapshotId;

    public PojoDataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            SimpleStats keyStats,
            SimpleStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            List<String> extraFiles,
            Timestamp creationTime,
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
        this(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols,
                null);
    }

    public PojoDataFileMeta(
            String fileName,
            long fileSize,
            long rowCount,
            BinaryRow minKey,
            BinaryRow maxKey,
            SimpleStats keyStats,
            SimpleStats valueStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            int level,
            List<String> extraFiles,
            Timestamp creationTime,
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols,
            @Nullable Long commitSnapshotId) {
        this.fileName = fileName;
        this.fileSize = fileSize;

        this.rowCount = rowCount;

        this.embeddedIndex = embeddedIndex;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.keyStats = keyStats;
        this.valueStats = valueStats;

        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.level = level;
        this.schemaId = schemaId;
        this.extraFiles = Collections.unmodifiableList(extraFiles);
        this.creationTime = creationTime;

        this.deleteRowCount = deleteRowCount;
        this.fileSource = fileSource;
        this.valueStatsCols = valueStatsCols;
        this.externalPath = externalPath;
        this.firstRowId = firstRowId;
        this.writeCols = writeCols;
        this.commitSnapshotId = commitSnapshotId;
    }

    @Override
    public String fileName() {
        return fileName;
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public Optional<Long> deleteRowCount() {
        return Optional.ofNullable(deleteRowCount);
    }

    @Override
    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    @Override
    public BinaryRow minKey() {
        return minKey;
    }

    @Override
    public BinaryRow maxKey() {
        return maxKey;
    }

    @Override
    public SimpleStats keyStats() {
        return keyStats;
    }

    @Override
    public SimpleStats valueStats() {
        return valueStats;
    }

    @Override
    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    @Override
    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    @Override
    public long schemaId() {
        return schemaId;
    }

    @Override
    public int level() {
        return level;
    }

    @Override
    public List<String> extraFiles() {
        return extraFiles;
    }

    @Override
    public Timestamp creationTime() {
        return creationTime;
    }

    @Override
    public long creationTimeEpochMillis() {
        return creationTime
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    @Override
    public String fileFormat() {
        String[] split = fileName.split("\\.");
        if (split.length == 1) {
            throw new RuntimeException("Can't find format from file: " + fileName());
        }
        return split[split.length - 1];
    }

    @Override
    public Optional<String> externalPath() {
        return Optional.ofNullable(externalPath);
    }

    @Override
    public Optional<String> externalPathDir() {
        return Optional.ofNullable(externalPath).map(Path::new).map(p -> p.getParent().toString());
    }

    @Override
    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    @Nullable
    public List<String> valueStatsCols() {
        return valueStatsCols;
    }

    @Nullable
    public Long firstRowId() {
        return firstRowId;
    }

    @Nullable
    public List<String> writeCols() {
        return writeCols;
    }

    @Override
    @Nullable
    public Long commitSnapshotId() {
        return commitSnapshotId;
    }

    @Override
    public PojoDataFileMeta assignCommitSnapshotId(long snapshotId) {
        return toBuilder().commitSnapshotId(snapshotId).build();
    }

    @Override
    public PojoDataFileMeta upgrade(int newLevel) {
        checkArgument(newLevel > this.level);
        return toBuilder().level(newLevel).build();
    }

    @Override
    public PojoDataFileMeta rename(String newFileName) {
        String newExternalPath = externalPathDir().map(dir -> dir + "/" + newFileName).orElse(null);
        return toBuilder().fileName(newFileName).externalPath(newExternalPath).build();
    }

    @Override
    public PojoDataFileMeta copyWithoutStats() {
        return toBuilder().valueStats(EMPTY_STATS).valueStatsCols(Collections.emptyList()).build();
    }

    @Override
    public PojoDataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        return toBuilder()
                .minSequenceNumber(minSequenceNumber)
                .maxSequenceNumber(maxSequenceNumber)
                .build();
    }

    @Override
    public PojoDataFileMeta assignFirstRowId(long firstRowId) {
        return toBuilder().firstRowId(firstRowId).build();
    }

    @Override
    public PojoDataFileMeta copy(List<String> newExtraFiles) {
        return toBuilder().extraFiles(newExtraFiles).build();
    }

    @Override
    public PojoDataFileMeta newExternalPath(String newExternalPath) {
        return toBuilder().externalPath(newExternalPath).build();
    }

    @Override
    public PojoDataFileMeta copy(byte[] newEmbeddedIndex) {
        return toBuilder().embeddedIndex(newEmbeddedIndex).build();
    }

    private Builder toBuilder() {
        return new Builder()
                .fileName(fileName)
                .fileSize(fileSize)
                .rowCount(rowCount)
                .minKey(minKey)
                .maxKey(maxKey)
                .keyStats(keyStats)
                .valueStats(valueStats)
                .minSequenceNumber(minSequenceNumber)
                .maxSequenceNumber(maxSequenceNumber)
                .schemaId(schemaId)
                .level(level)
                .extraFiles(extraFiles)
                .creationTime(creationTime)
                .deleteRowCount(deleteRowCount)
                .embeddedIndex(embeddedIndex)
                .fileSource(fileSource)
                .valueStatsCols(valueStatsCols)
                .externalPath(externalPath)
                .firstRowId(firstRowId)
                .writeCols(writeCols)
                .commitSnapshotId(commitSnapshotId);
    }

    private static class Builder {
        private String fileName;
        private long fileSize;
        private long rowCount;
        private BinaryRow minKey;
        private BinaryRow maxKey;
        private SimpleStats keyStats;
        private SimpleStats valueStats;
        private long minSequenceNumber;
        private long maxSequenceNumber;
        private long schemaId;
        private int level;
        private List<String> extraFiles;
        private Timestamp creationTime;
        private @Nullable Long deleteRowCount;
        private @Nullable byte[] embeddedIndex;
        private @Nullable FileSource fileSource;
        private @Nullable List<String> valueStatsCols;
        private @Nullable String externalPath;
        private @Nullable Long firstRowId;
        private @Nullable List<String> writeCols;
        private @Nullable Long commitSnapshotId;

        Builder fileName(String v) {
            this.fileName = v;
            return this;
        }

        Builder fileSize(long v) {
            this.fileSize = v;
            return this;
        }

        Builder rowCount(long v) {
            this.rowCount = v;
            return this;
        }

        Builder minKey(BinaryRow v) {
            this.minKey = v;
            return this;
        }

        Builder maxKey(BinaryRow v) {
            this.maxKey = v;
            return this;
        }

        Builder keyStats(SimpleStats v) {
            this.keyStats = v;
            return this;
        }

        Builder valueStats(SimpleStats v) {
            this.valueStats = v;
            return this;
        }

        Builder minSequenceNumber(long v) {
            this.minSequenceNumber = v;
            return this;
        }

        Builder maxSequenceNumber(long v) {
            this.maxSequenceNumber = v;
            return this;
        }

        Builder schemaId(long v) {
            this.schemaId = v;
            return this;
        }

        Builder level(int v) {
            this.level = v;
            return this;
        }

        Builder extraFiles(List<String> v) {
            this.extraFiles = v;
            return this;
        }

        Builder creationTime(Timestamp v) {
            this.creationTime = v;
            return this;
        }

        Builder deleteRowCount(@Nullable Long v) {
            this.deleteRowCount = v;
            return this;
        }

        Builder embeddedIndex(@Nullable byte[] v) {
            this.embeddedIndex = v;
            return this;
        }

        Builder fileSource(@Nullable FileSource v) {
            this.fileSource = v;
            return this;
        }

        Builder valueStatsCols(@Nullable List<String> v) {
            this.valueStatsCols = v;
            return this;
        }

        Builder externalPath(@Nullable String v) {
            this.externalPath = v;
            return this;
        }

        Builder firstRowId(@Nullable Long v) {
            this.firstRowId = v;
            return this;
        }

        Builder writeCols(@Nullable List<String> v) {
            this.writeCols = v;
            return this;
        }

        Builder commitSnapshotId(@Nullable Long v) {
            this.commitSnapshotId = v;
            return this;
        }

        PojoDataFileMeta build() {
            return new PojoDataFileMeta(
                    fileName,
                    fileSize,
                    rowCount,
                    minKey,
                    maxKey,
                    keyStats,
                    valueStats,
                    minSequenceNumber,
                    maxSequenceNumber,
                    schemaId,
                    level,
                    extraFiles,
                    creationTime,
                    deleteRowCount,
                    embeddedIndex,
                    fileSource,
                    valueStatsCols,
                    externalPath,
                    firstRowId,
                    writeCols,
                    commitSnapshotId);
        }
    }

    @Override
    public RoaringBitmap32 toFileSelection(List<Range> rowRanges) {
        RoaringBitmap32 selection = null;
        if (rowRanges != null) {
            if (firstRowId() == null) {
                throw new IllegalStateException(
                        "firstRowId is null, can't convert to file selection");
            }
            selection = new RoaringBitmap32();
            Range fileRange = nonNullRowIdRange();
            List<Range> result = new ArrayList<>();
            for (Range expected : rowRanges) {
                Range intersection = Range.intersection(fileRange, expected);
                if (intersection != null) {
                    result.add(intersection);
                }
            }

            if (result.size() == 1 && result.get(0).equals(fileRange)) {
                return null;
            }

            for (Range range : result) {
                for (long rowId = range.from; rowId <= range.to; rowId++) {
                    selection.add((int) (rowId - fileRange.from));
                }
            }
        }
        return selection;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof DataFileMeta)) {
            return false;
        }
        DataFileMeta that = (DataFileMeta) o;
        return Objects.equals(fileName, that.fileName())
                && fileSize == that.fileSize()
                && rowCount == that.rowCount()
                && Arrays.equals(embeddedIndex, that.embeddedIndex())
                && Objects.equals(minKey, that.minKey())
                && Objects.equals(maxKey, that.maxKey())
                && Objects.equals(keyStats, that.keyStats())
                && Objects.equals(valueStats, that.valueStats())
                && minSequenceNumber == that.minSequenceNumber()
                && maxSequenceNumber == that.maxSequenceNumber()
                && schemaId == that.schemaId()
                && level == that.level()
                && Objects.equals(extraFiles, that.extraFiles())
                && Objects.equals(creationTime, that.creationTime())
                && Objects.equals(deleteRowCount, that.deleteRowCount().orElse(null))
                && Objects.equals(fileSource, that.fileSource().orElse(null))
                && Objects.equals(valueStatsCols, that.valueStatsCols())
                && Objects.equals(externalPath, that.externalPath().orElse(null))
                && Objects.equals(firstRowId, that.firstRowId())
                && Objects.equals(writeCols, that.writeCols())
                && Objects.equals(commitSnapshotId, that.commitSnapshotId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                fileName,
                fileSize,
                rowCount,
                Arrays.hashCode(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols,
                commitSnapshotId);
    }

    @Override
    public String toString() {
        return String.format(
                "{fileName: %s, fileSize: %d, rowCount: %d, embeddedIndex: %s, "
                        + "minKey: %s, maxKey: %s, keyStats: %s, valueStats: %s, "
                        + "minSequenceNumber: %d, maxSequenceNumber: %d, "
                        + "schemaId: %d, level: %d, extraFiles: %s, creationTime: %s, "
                        + "deleteRowCount: %d, fileSource: %s, valueStatsCols: %s, externalPath: %s, firstRowId: %s, writeCols: %s, commitSnapshotId: %s}",
                fileName,
                fileSize,
                rowCount,
                Arrays.toString(embeddedIndex),
                minKey,
                maxKey,
                keyStats,
                valueStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols,
                commitSnapshotId);
    }
}
