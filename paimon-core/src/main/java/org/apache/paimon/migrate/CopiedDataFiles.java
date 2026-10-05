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

import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.SpecialFields;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * Adapts the metadata of data files copied from another table, as {@code sys.copy} does, to the
 * table they are committed into.
 *
 * <ul>
 *   <li>Row ids: a copied file keeps the first row id it had in the source table. When the target
 *       has given out those row ids already, for example to rows of a partition the copy does not
 *       overwrite, the copied rows would share row ids with them. All copied row ids are then
 *       shifted by the same offset, so that they follow the row ids of the target and keep their
 *       layout: files that update columns of the same rows keep sharing a row id range.
 *   <li>Sequence numbers: a data-evolution read takes each column from the file with the highest
 *       sequence number, and a commit stamps new files with its snapshot id. Copied files keep the
 *       sequence numbers of the source, which can exceed the snapshot ids of the target, so later
 *       updates in the target would be ignored. On a data-evolution target, the copied sequence
 *       numbers are mapped, in order, to values up to the snapshot id of the copy: the newest
 *       becomes 0, which the commit stamps with its snapshot id, older ones -1, -2 and so on.
 * </ul>
 */
public final class CopiedDataFiles {

    private CopiedDataFiles() {}

    /** Replaces, in place, every copied file by its metadata for {@code target}. */
    public static void adaptToTarget(
            FileStoreTable target, Collection<List<DataFileMeta>> copiedFilesByBucket) {
        List<DataFileMeta> copied = new ArrayList<>();
        copiedFilesByBucket.forEach(copied::addAll);
        if (copied.isEmpty()) {
            return;
        }
        long rowIdShift = rowIdShift(target, copied);
        if (target.coreOptions().rowTrackingEnabled() || rowIdShift != 0) {
            checkNoFileStoresRowIds(target, copied);
        }
        Map<Long, Long> sequences =
                target.coreOptions().dataEvolutionEnabled() ? sequenceMapping(copied) : null;
        if (rowIdShift == 0 && sequences == null) {
            return;
        }
        for (List<DataFileMeta> files : copiedFilesByBucket) {
            ListIterator<DataFileMeta> iterator = files.listIterator();
            while (iterator.hasNext()) {
                iterator.set(adapt(iterator.next(), rowIdShift, sequences));
            }
        }
    }

    private static DataFileMeta adapt(
            DataFileMeta file, long rowIdShift, @Nullable Map<Long, Long> sequences) {
        DataFileMeta result = file;
        if (rowIdShift != 0 && file.firstRowId() != null) {
            result = result.newFirstRowId(file.firstRowId() + rowIdShift);
        }
        if (sequences != null) {
            result =
                    result.assignSequenceNumber(
                            sequences.get(file.minSequenceNumber()),
                            sequences.get(file.maxSequenceNumber()));
            long[] writeColsSequences = file.writeColsSequences();
            if (writeColsSequences != null) {
                long[] mapped = new long[writeColsSequences.length];
                for (int i = 0; i < writeColsSequences.length; i++) {
                    mapped[i] = sequences.get(writeColsSequences[i]);
                }
                result = result.withWriteColsSequences(mapped);
            }
        }
        return result;
    }

    /**
     * The offset that moves the smallest copied row id past every row id the target has given out:
     * its next row id, and the row ids of its live files, which a copy may have committed without
     * advancing it.
     */
    private static long rowIdShift(FileStoreTable target, List<DataFileMeta> copied) {
        long minCopied = Long.MAX_VALUE;
        for (DataFileMeta file : copied) {
            if (file.firstRowId() != null) {
                minCopied = Math.min(minCopied, file.firstRowId());
            }
        }
        if (minCopied == Long.MAX_VALUE) {
            return 0;
        }
        Snapshot latest = target.snapshotManager().latestSnapshot();
        if (latest == null) {
            return 0;
        }
        long used = latest.nextRowId() == null ? 0 : latest.nextRowId();
        Iterator<DataFileMeta> live = liveFiles(target, latest);
        while (live.hasNext()) {
            DataFileMeta file = live.next();
            if (file.firstRowId() != null) {
                used = Math.max(used, file.firstRowId() + file.rowCount());
            }
        }
        if (minCopied >= used) {
            return 0;
        }

        return used - minCopied;
    }

    /**
     * A file that stores the row ids of its rows, as a copy-on-write update writes it, has no first
     * row id. Its row ids are in its data: they cannot be shifted, and a commit does not move the
     * next row id of a row-tracking table past them, so rows written later would get them again.
     */
    private static void checkNoFileStoresRowIds(FileStoreTable target, List<DataFileMeta> copied) {
        for (DataFileMeta file : copied) {
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

    private static Iterator<DataFileMeta> liveFiles(FileStoreTable target, Snapshot snapshot) {
        Iterator<org.apache.paimon.manifest.ManifestEntry> entries =
                target.newSnapshotReader().withSnapshot(snapshot).readFileIterator();
        return new Iterator<DataFileMeta>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public DataFileMeta next() {
                return entries.next().file();
            }
        };
    }

    /** Maps every sequence value of the copied files, newest first, to 0, -1, -2 and so on. */
    private static Map<Long, Long> sequenceMapping(List<DataFileMeta> copied) {
        TreeSet<Long> values = new TreeSet<>();
        for (DataFileMeta file : copied) {
            values.add(file.minSequenceNumber());
            values.add(file.maxSequenceNumber());
            long[] writeColsSequences = file.writeColsSequences();
            if (writeColsSequences != null) {
                for (long value : writeColsSequences) {
                    values.add(value);
                }
            }
        }
        Map<Long, Long> mapping = new HashMap<>();
        long mapped = 0;
        for (Long value : values.descendingSet()) {
            mapping.put(value, mapped--);
        }
        return mapping;
    }
}
