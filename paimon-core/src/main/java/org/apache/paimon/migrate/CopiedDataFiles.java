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

package org.apache.paimon.migrate;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.PojoDataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.types.VectorType.isVectorStoreFile;

/**
 * Prepares data files copied from another table, as {@code sys.copy} does, for the commit into the
 * table they are copied to.
 *
 * <p>A copied file arrives with the first row id and the sequence numbers it had in the source
 * table. Row ids identify rows within one table, and the target has its own: committing the source
 * ones would let the copied rows share row ids with rows of the target, and on a data-evolution
 * target the source sequence numbers could hide later updates. The copied files are therefore
 * committed like files written for the target: without a first row id and with sequence numbers
 * that the commit stamps with its snapshot id. A row-tracking commit then assigns the row ids,
 * against the snapshot it commits on, as it does for any writer, so a concurrent commit to the
 * target cannot take the same row ids.
 *
 * <p>A row-tracking commit gives a blob or vector-store file the row ids of the normal file before
 * it, so the files of one row id range are committed in order: the normal file, then its dedicated
 * files. A file that updates columns of rows another copied file holds cannot be committed that
 * way, and is refused: compacting the source first merges it into one file per row id range. So is
 * a blob or vector-store file that the commit would give the row ids of other rows than it holds.
 */
public final class CopiedDataFiles {

    private CopiedDataFiles() {}

    /** Replaces, in place, every copied file by its metadata for {@code target}. */
    public static void adaptToTarget(
            FileStoreTable target, Collection<List<DataFileMeta>> copiedFilesByBucket) {
        boolean rowTracking = target.coreOptions().rowTrackingEnabled();
        boolean vectorStoreFiles =
                copiedFilesByBucket.stream()
                        .flatMap(List::stream)
                        .anyMatch(file -> kind(file) == 2);
        for (List<DataFileMeta> files : copiedFilesByBucket) {
            checkNoColumnUpdates(target, files);
            if (rowTracking) {
                checkNoFileStoresRowIds(target, files);
                // a normal file before the dedicated files of its row id range
                files.sort(
                        Comparator.comparing(
                                        (DataFileMeta file) -> file.firstRowId(),
                                        Comparator.nullsLast(Comparator.naturalOrder()))
                                .thenComparingInt(CopiedDataFiles::kind));
                checkDedicatedFilesFollowTheirRows(target, files, vectorStoreFiles);
            }
            ListIterator<DataFileMeta> iterator = files.listIterator();
            while (iterator.hasNext()) {
                DataFileMeta file = iterator.next();
                // A table without row tracking keeps no row ids: its own files have none either.
                iterator.set(rowTracking ? asNewFile(file) : file.newFirstRowId(null));
            }
        }
    }

    /**
     * A file written for the target: no first row id, an append so that the commit assigns one, and
     * sequence numbers starting at 0 so that the commit stamps them with its snapshot id.
     */
    private static DataFileMeta asNewFile(DataFileMeta file) {
        return new PojoDataFileMeta(
                file.fileName(),
                file.fileSize(),
                file.rowCount(),
                file.minKey(),
                file.maxKey(),
                file.keyStats(),
                file.valueStats(),
                0L,
                0L,
                file.schemaId(),
                file.level(),
                file.extraFiles(),
                file.creationTime(),
                file.deleteRowCount().orElse(null),
                file.embeddedIndex(),
                FileSource.APPEND,
                file.valueStatsCols(),
                file.externalPath().orElse(null),
                null,
                file.writeCols(),
                null);
    }

    /**
     * Refuses a normal file whose row ids another normal file of the copy holds too: a column
     * update of a data-evolution table, which only shares the row ids of the file it updates.
     */
    private static void checkNoColumnUpdates(FileStoreTable target, List<DataFileMeta> files) {
        List<DataFileMeta> normal = new ArrayList<>();
        for (DataFileMeta file : files) {
            if (file.firstRowId() != null && kind(file) == 0) {
                normal.add(file);
            }
        }
        normal.sort(Comparator.comparingLong(DataFileMeta::nonNullFirstRowId));
        for (int i = 1; i < normal.size(); i++) {
            DataFileMeta previous = normal.get(i - 1);
            DataFileMeta current = normal.get(i);
            if (current.nonNullFirstRowId() < previous.nonNullFirstRowId() + previous.rowCount()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot copy data files %s and %s into table %s: they hold the "
                                        + "same rows, one updating columns of the other. Compact "
                                        + "the source table first, which merges them.",
                                previous.fileName(), current.fileName(), target.name()));
            }
        }
    }

    /**
     * A file that stores the row ids of its rows, as a copy-on-write update writes it, has no first
     * row id: its row ids are in its data and would collide with the row ids of the target.
     */
    private static void checkNoFileStoresRowIds(FileStoreTable target, List<DataFileMeta> files) {
        for (DataFileMeta file : files) {
            List<String> writeCols = file.writeCols();
            if (writeCols != null && writeCols.contains(SpecialFields.ROW_ID.name())) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot copy data file %s into table %s: its rows store their row "
                                        + "ids, written by a copy-on-write UPDATE, DELETE or MERGE "
                                        + "INTO on the source, so they cannot be given row ids of "
                                        + "the target. Rewrite those rows in the source first, for "
                                        + "example with INSERT OVERWRITE.",
                                file.fileName(), target.name()));
            }
        }
    }

    /**
     * Refuses a blob or vector-store file that the commit would give the row ids of other rows than
     * it holds. The commit gives a normal file the row ids after the normal file before it, a blob
     * file those from the start of the normal file before it, or after the blob file of the same
     * field following that normal file, and a vector-store file those after the vector-store file
     * before it in the whole commit. This replays that on the files of one bucket, which the commit
     * keeps together but may order among the other buckets in any way: so when the copy has
     * vector-store files, those of every bucket must also hold as many rows as its normal files. A
     * source breaks this when a blob or vector column was updated, or added after rows were
     * written.
     */
    private static void checkDedicatedFilesFollowTheirRows(
            FileStoreTable target, List<DataFileMeta> files, boolean vectorStoreFiles) {
        // the row ids the commit gives, counted from the first one it gives to the bucket
        long start = 0;
        long blobStartDefault = 0;
        Map<String, Long> blobStarts = new HashMap<>();
        long vectorStoreStart = 0;
        List<DataFileMeta> normalFiles = new ArrayList<>();
        List<Long> normalStarts = new ArrayList<>();
        List<DataFileMeta> dedicatedFiles = new ArrayList<>();
        List<Long> dedicatedStarts = new ArrayList<>();
        for (DataFileMeta file : files) {
            int kind = kind(file);
            if (kind == 0) {
                normalFiles.add(file);
                normalStarts.add(start);
                blobStartDefault = start;
                blobStarts.clear();
                start += file.rowCount();
                continue;
            }
            long given;
            if (kind == 1) {
                String field = file.writeCols().get(0);
                given = blobStarts.getOrDefault(field, blobStartDefault);
                blobStarts.put(field, given + file.rowCount());
            } else {
                given = vectorStoreStart;
                vectorStoreStart += file.rowCount();
            }
            if (given >= start) {
                // no normal file holds these row ids yet: the commit refuses it
                throw rowsOfOtherFiles(target, file);
            }
            dedicatedFiles.add(file);
            dedicatedStarts.add(given);
        }

        // the normal and the dedicated files are both in the order of their source row ids
        int normal = 0;
        for (int i = 0; i < dedicatedFiles.size(); i++) {
            DataFileMeta file = dedicatedFiles.get(i);
            if (file.firstRowId() == null) {
                continue;
            }
            long row = file.firstRowId();
            long end = row + file.rowCount();
            long offset = dedicatedStarts.get(i) - row;
            while (normal < normalFiles.size() && endRowId(normalFiles.get(normal)) <= row) {
                normal++;
            }
            // every row of the file must get the row id the commit gives it in its normal file
            for (int n = normal; row < end; n++) {
                DataFileMeta normalFile = n < normalFiles.size() ? normalFiles.get(n) : null;
                if (normalFile == null
                        || normalFile.firstRowId() == null
                        || normalFile.firstRowId() > row
                        || normalStarts.get(n) - normalFile.firstRowId() != offset) {
                    throw rowsOfOtherFiles(target, file);
                }
                row = endRowId(normalFile);
            }
        }

        if (vectorStoreFiles && vectorStoreStart < start) {
            // rows without a vector-store file move those of the next buckets onto other rows
            for (int n = normalFiles.size() - 1; n >= 0; n--) {
                if (normalStarts.get(n) <= vectorStoreStart) {
                    throw rowsOfOtherFiles(target, normalFiles.get(n));
                }
            }
        }
    }

    private static long endRowId(DataFileMeta file) {
        return file.firstRowId() == null ? Long.MIN_VALUE : file.firstRowId() + file.rowCount();
    }

    private static IllegalArgumentException rowsOfOtherFiles(
            FileStoreTable target, DataFileMeta file) {
        return new IllegalArgumentException(
                String.format(
                        "Cannot copy data file %s into table %s: the commit would give the blob or "
                                + "vector-store files of the source the row ids of other rows "
                                + "than they hold, because they do not follow the rows of the "
                                + "normal files, as after updating a blob or vector column or "
                                + "adding one to rows already written. Rewrite those rows in "
                                + "the source first, for example with INSERT OVERWRITE.",
                        file.fileName(), target.name()));
    }

    /** 0 for a normal file, 1 for a blob file, 2 for a vector-store file. */
    private static int kind(DataFileMeta file) {
        if (isBlobFile(file.fileName())) {
            return 1;
        }
        return isVectorStoreFile(file.fileName()) ? 2 : 0;
    }
}
