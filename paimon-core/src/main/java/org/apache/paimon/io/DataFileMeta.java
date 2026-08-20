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
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
public interface DataFileMeta {

    String FILE_NAME = "_FILE_NAME";
    String FILE_SIZE = "_FILE_SIZE";
    String ROW_COUNT = "_ROW_COUNT";
    String MIN_KEY = "_MIN_KEY";
    String MAX_KEY = "_MAX_KEY";
    String KEY_STATS = "_KEY_STATS";
    String VALUE_STATS = "_VALUE_STATS";
    String MIN_SEQUENCE_NUMBER = "_MIN_SEQUENCE_NUMBER";
    String MAX_SEQUENCE_NUMBER = "_MAX_SEQUENCE_NUMBER";
    String SCHEMA_ID = "_SCHEMA_ID";
    String LEVEL = "_LEVEL";
    String EXTRA_FILES = "_EXTRA_FILES";
    String CREATION_TIME = "_CREATION_TIME";
    String DELETE_ROW_COUNT = "_DELETE_ROW_COUNT";
    String EMBEDDED_FILE_INDEX = "_EMBEDDED_FILE_INDEX";
    String FILE_SOURCE = "_FILE_SOURCE";
    String VALUE_STATS_COLS = "_VALUE_STATS_COLS";
    String EXTERNAL_PATH = "_EXTERNAL_PATH";
    String FIRST_ROW_ID = "_FIRST_ROW_ID";
    String WRITE_COLS = "_WRITE_COLS";

    RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, FILE_NAME, newStringType(false)),
                            new DataField(1, FILE_SIZE, new BigIntType(false)),
                            new DataField(2, ROW_COUNT, new BigIntType(false)),
                            new DataField(3, MIN_KEY, newBytesType(false)),
                            new DataField(4, MAX_KEY, newBytesType(false)),
                            new DataField(5, KEY_STATS, SimpleStats.SCHEMA),
                            new DataField(6, VALUE_STATS, SimpleStats.SCHEMA),
                            new DataField(7, MIN_SEQUENCE_NUMBER, new BigIntType(false)),
                            new DataField(8, MAX_SEQUENCE_NUMBER, new BigIntType(false)),
                            new DataField(9, SCHEMA_ID, new BigIntType(false)),
                            new DataField(10, LEVEL, new IntType(false)),
                            new DataField(
                                    11, EXTRA_FILES, new ArrayType(false, newStringType(false))),
                            new DataField(12, CREATION_TIME, DataTypes.TIMESTAMP_MILLIS()),
                            new DataField(13, DELETE_ROW_COUNT, new BigIntType(true)),
                            new DataField(14, EMBEDDED_FILE_INDEX, newBytesType(true)),
                            new DataField(15, FILE_SOURCE, new TinyIntType(true)),
                            new DataField(
                                    16,
                                    VALUE_STATS_COLS,
                                    DataTypes.ARRAY(DataTypes.STRING().notNull())),
                            new DataField(17, EXTERNAL_PATH, newStringType(true)),
                            new DataField(18, FIRST_ROW_ID, new BigIntType(true)),
                            new DataField(
                                    19, WRITE_COLS, new ArrayType(true, newStringType(false)))));

    BinaryRow EMPTY_MIN_KEY = EMPTY_ROW;
    BinaryRow EMPTY_MAX_KEY = EMPTY_ROW;
    int DUMMY_LEVEL = 0;

    static DataFileMeta forAppend(
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
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
        return new PojoDataFileMeta(
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
                externalPath,
                firstRowId,
                writeCols);
    }

    static DataFileMeta create(
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
            @Nullable List<String> valueStatsCols,
            @Nullable String externalPath,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
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
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                externalPath,
                firstRowId,
                writeCols);
    }

    static DataFileMeta create(
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
            @Nullable List<String> valueStatsCols,
            @Nullable Long firstRowId,
            @Nullable List<String> writeCols) {
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
                Collections.emptyList(),
                Timestamp.fromLocalDateTime(LocalDateTime.now()).toMillisTimestamp(),
                deleteRowCount,
                embeddedIndex,
                fileSource,
                valueStatsCols,
                null,
                firstRowId,
                writeCols);
    }

    static DataFileMeta create(
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
                writeCols);
    }

    String fileName();

    long fileSize();

    long rowCount();

    Optional<Long> deleteRowCount();

    byte[] embeddedIndex();

    BinaryRow minKey();

    BinaryRow maxKey();

    SimpleStats keyStats();

    SimpleStats valueStats();

    /**
     * Minimum sequence number of records in this file. When {@code sequence.snapshot-ordering} is
     * enabled for a primary-key table, this field is repurposed to carry the commit snapshot id
     * instead of the per-record sequence number range (the snapshot id is stamped into it at commit
     * time by {@code FileStoreCommitImpl}).
     */
    long minSequenceNumber();

    /** @see #minSequenceNumber() */
    long maxSequenceNumber();

    long schemaId();

    int level();

    List<String> extraFiles();

    Timestamp creationTime();

    default long creationTimeEpochMillis() {
        return creationTime()
                .toLocalDateTime()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }

    String fileFormat();

    Optional<String> externalPath();

    Optional<String> externalPathDir();

    Optional<FileSource> fileSource();

    @Nullable
    List<String> valueStatsCols();

    @Nullable
    Long firstRowId();

    default long nonNullFirstRowId() {
        Long firstRowId = firstRowId();
        checkArgument(firstRowId != null, "First row id should not be null.");
        return firstRowId;
    }

    default Range nonNullRowIdRange() {
        long firstRowId = nonNullFirstRowId();
        return new Range(firstRowId, firstRowId + rowCount() - 1);
    }

    @Nullable
    List<String> writeCols();

    DataFileMeta upgrade(int newLevel);

    DataFileMeta rename(String newFileName);

    DataFileMeta copyWithoutStats();

    DataFileMeta assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber);

    DataFileMeta assignFirstRowId(long firstRowId);

    DataFileMeta newFirstRowId(@Nullable Long newFirstRowId);

    default List<Path> collectFiles(DataFilePathFactory pathFactory) {
        List<Path> paths = new ArrayList<>();
        paths.add(pathFactory.toPath(this));
        extraFiles().forEach(f -> paths.add(pathFactory.toAlignedPath(f, this)));
        return paths;
    }

    DataFileMeta copy(List<String> newExtraFiles);

    DataFileMeta newExternalPath(String newExternalPath);

    DataFileMeta copy(byte[] newEmbeddedIndex);

    default RoaringBitmap32 toFileSelection(List<Range> rowRanges) {
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

    static long getMaxSequenceNumber(List<DataFileMeta> fileMetas) {
        return fileMetas.stream()
                .map(DataFileMeta::maxSequenceNumber)
                .max(Long::compare)
                .orElse(-1L);
    }

    static Map<Integer, List<DataFileMeta>> groupByLevel(List<DataFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(DataFileMeta::level, Collectors.toList()));
    }
}
