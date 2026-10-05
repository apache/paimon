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
import java.util.List;
import java.util.ListIterator;

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
 * way, and is refused: compacting the source first merges it into one file per row id range.
 */
public final class CopiedDataFiles {

    private CopiedDataFiles() {}

    /** Replaces, in place, every copied file by its metadata for {@code target}. */
    public static void adaptToTarget(
            FileStoreTable target, Collection<List<DataFileMeta>> copiedFilesByBucket) {
        boolean rowTracking = target.coreOptions().rowTrackingEnabled();
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

    /** 0 for a normal file, 1 for a blob file, 2 for a vector-store file. */
    private static int kind(DataFileMeta file) {
        if (isBlobFile(file.fileName())) {
            return 1;
        }
        return isVectorStoreFile(file.fileName()) ? 2 : 0;
    }
}
