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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.stats.SimpleStats.EMPTY_STATS;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SerializationUtils.newBytesType;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * Metadata of a data file.
 *
 * @since 0.9.0
 */
@Public
public class DataFileMeta {

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_FILE_NAME", newStringType(false)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_ROW_COUNT", new BigIntType(false)),
                            new DataField(3, "_MIN_KEY", newBytesType(false)),
                            new DataField(4, "_MAX_KEY", newBytesType(false)),
                            new DataField(5, "_KEY_STATS", SimpleStats.SCHEMA),
                            new DataField(6, "_VALUE_STATS", SimpleStats.SCHEMA),
                            new DataField(7, "_MIN_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(8, "_MAX_SEQUENCE_NUMBER", new BigIntType(false)),
                            new DataField(9, "_SCHEMA_ID", new BigIntType(false)),
                            new DataField(10, "_LEVEL", new IntType(false)),
                            new DataField(
                                    11, "_EXTRA_FILES", new ArrayType(false, newStringType(false))),
                            new DataField(12, "_CREATION_TIME", DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(13, "_DELETE_ROW_COUNT", new BigIntType(true)),
                            new DataField(14, "_EMBEDDED_FILE_INDEX", newBytesType(true)),
                            new DataField(15, "_FILE_SOURCE", new TinyIntType(true)),
                            new DataField(
                                    16,
                                    "_VALUE_STATS_COLS",
                                    DataTypes.ARRAY(DataTypes.STRING().notNull())),
                            new DataField(17, "_EXTERNAL_PATH", newStringType(true))));

    public static final BinaryRow EMPTY_MIN_KEY = EMPTY_ROW;
    public static final BinaryRow EMPTY_MAX_KEY = EMPTY_ROW;
    public static final int DUMMY_LEVEL = 0;

    private final String fileName;
    private final long fileSize;

    // total number of rows (including add & delete) in this file
    private final long rowCount;

    private final BinaryRow minKey;
    private final BinaryRow maxKey;
    private final SimpleStats keyStats;
    private final SimpleStats valueStats;

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

    public static DataFileMeta forAppend(
            String fileName,
            long fileSize,
            long rowCount,
            SimpleStats rowStats,
            long minSequenceNumber,
            long maxSequenceNumber,
            long schemaId,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols) {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                EMPTY_MIN_KEY,
                EMPTY_MAX_KEY,
                EMPTY_STATS,
                rowStats,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                DUMMY_LEVEL,
                extraFiles,
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                0L,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                null);
    }

    public DataFileMeta(
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
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols) {
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
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                null);
    }

    public DataFileMeta(
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
            @Nullable Long deleteRowCount,
            @Nullable byte[] embeddedIndex,
            @Nullable FileSource fileSource,
            @Nullable List<String> valueStatsCols) {
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
                Collections.emptyList(),
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                null);
    }

    public DataFileMeta(
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
            @Nullable String externalPath) {
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
    }

    public String fileName() {
        return fileName;
    }

    public long fileSize() {
        return fileSize;
    }

    public long rowCount() {
        return rowCount;
    }

    public Optional<Long> addRowCount() {
        return Optional.ofNullable(deleteRowCount).map(c -> rowCount - c);
    }

    public Optional<Long> deleteRowCount() {
        return Optional.ofNullable(deleteRowCount);
    }

    public byte[] embeddedIndex() {
        return embeddedIndex;
    }

    public BinaryRow minKey() {
        return minKey;
    }

    public BinaryRow maxKey() {
        return maxKey;
    }

    public SimpleStats keyStats() {
        return keyStats;
    }

    public SimpleStats valueStats() {
        return valueStats;
    }

    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    public long maxSequenceNumber() {
        return maxSequenceNumber;
    }

    public long schemaId() {
        return schemaId;
    }

    public int level() {
        return level;
    }

    /**
     * Usage:
     *
     * <ul>
     *   <li>Paimon 0.2
     *       <ul>
     *         <li>Stores changelog files for {@link CoreOptions.ChangelogProducer#INPUT}. Changelog
     *             files are moved to {@link DataIncrement} since Paimon 0.3.
     *       </ul>
     * </ul>
     */
    public List<String> extraFiles() {
        return extraFiles;
    }

    public Timestamp creationTime() {
        return creationTime;
    }

    public long creationTimeEpochMillis() {
        return creationTime
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    public String fileFormat() {
        String[] split = fileName.split("\\.");
        if (split.length == 1) {
            throw new RuntimeException("Can't find format from file: " + fileName());
        }
        return split[split.length - 1];
    }

    @Nullable
    public String externalPath() {
        return externalPath;
    }

    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    @Nullable
    public List<String> valueStatsCols() {
        return valueStatsCols;
    }

    public DataFileMeta upgrade(int newLevel) {
        checkArgument(newLevel > this.level);
        return new DataFileMeta(
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
                newLevel,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath);
    }

    public DataFileMeta rename(String newFileName) {
        return new DataFileMeta(
                newFileName,
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
                externalPath);
    }

    public DataFileMeta copyWithoutStats() {
        return new DataFileMeta(
                fileName,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                keyStats,
                EMPTY_STATS,
                minSequenceNumber,
                maxSequenceNumber,
                schemaId,
                level,
                extraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                Collections.emptyList(),
                externalPath);
    }

    public List<Path> collectFiles(DataFilePathFactory pathFactory) {
        List<Path> paths = new ArrayList<>();
        paths.add(pathFactory.toPath(fileName, externalPath));
        extraFiles.forEach(f -> paths.add(pathFactory.toPath(f, externalPath)));
        return paths;
    }

    public DataFileMeta copy(List<String> newExtraFiles) {
        return new DataFileMeta(
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
                newExtraFiles,
                creationTime,
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath);
    }

    public DataFileMeta copy(byte[] newEmbeddedIndex) {
        return new DataFileMeta(
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
                newEmbeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath);
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
        return Objects.equals(fileName, that.fileName)
                && fileSize == that.fileSize
                && rowCount == that.rowCount
                && Arrays.equals(embeddedIndex, that.embeddedIndex)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(keyStats, that.keyStats)
                && Objects.equals(valueStats, that.valueStats)
                && minSequenceNumber == that.minSequenceNumber
                && maxSequenceNumber == that.maxSequenceNumber
                && schemaId == that.schemaId
                && level == that.level
                && Objects.equals(extraFiles, that.extraFiles)
                && Objects.equals(creationTime, that.creationTime)
                && Objects.equals(deleteRowCount, that.deleteRowCount)
                && Objects.equals(fileSource, that.fileSource)
                && Objects.equals(valueStatsCols, that.valueStatsCols)
                && Objects.equals(externalPath, that.externalPath);
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
                externalPath);
    }

    @Override
    public String toString() {
        return String.format(
                "{fileName: %s, fileSize: %d, rowCount: %d, embeddedIndex: %s, "
                        + "minKey: %s, maxKey: %s, keyStats: %s, valueStats: %s, "
                        + "minSequenceNumber: %d, maxSequenceNumber: %d, "
                        + "schemaId: %d, level: %d, extraFiles: %s, creationTime: %s, "
                        + "deleteRowCount: %d, fileSource: %s, valueStatsCols: %s, externalPath: %s}",
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
                externalPath);
    }

    public static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }
}
