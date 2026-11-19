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

package org.apache.paimon.mergetree.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Files of one bucket of a primary-key table with data evolution, split into full-row base files
 * and the column files attached to each base file.
 *
 * <p>A column file holds some non-key columns of every row of exactly one base file. Both share
 * {@code firstRowId} and {@code rowCount}, so the n-th row of the column file belongs to the n-th
 * row of the base file.
 *
 * <p>The base files never change after grouping. The column files are read by compaction threads
 * while the writer thread drops them, and a cancelled compaction may still be running when the next
 * one starts, so they are kept in a concurrent map.
 */
public class ColumnFileGroups {

    private final List<DataFileMeta> baseFiles;
    private final Map<String, List<DataFileMeta>> columnFiles;

    private ColumnFileGroups(
            List<DataFileMeta> baseFiles, Map<String, List<DataFileMeta>> columnFiles) {
        this.baseFiles = baseFiles;
        this.columnFiles = columnFiles;
    }

    public static boolean isColumnFile(DataFileMeta file) {
        return file.isPartialColumnFile();
    }

    public static boolean isColumnFile(@Nullable List<String> writeCols, BinaryRow minKey) {
        return writeCols != null && minKey.getFieldCount() == 0;
    }

    public static boolean hasColumnFiles(List<DataFileMeta> files) {
        for (DataFileMeta file : files) {
            if (isColumnFile(file)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Groups the files of one bucket.
     *
     * @throws IllegalStateException if a column file has no base file, or the bucket also holds
     *     files written before data evolution was enabled.
     */
    public static ColumnFileGroups group(List<DataFileMeta> files) {
        return group(files, false);
    }

    /**
     * Groups the files of one bucket.
     *
     * @param allowLegacyFiles whether files without alignment range may be grouped together with
     *     column files, for example to migrate them by compaction.
     */
    public static ColumnFileGroups group(List<DataFileMeta> files, boolean allowLegacyFiles) {
        List<DataFileMeta> baseFiles = new ArrayList<>();
        Map<Long, DataFileMeta> basesByFirstRowId = new HashMap<>();
        List<DataFileMeta> columns = new ArrayList<>();
        DataFileMeta legacyFile = null;
        for (DataFileMeta file : files) {
            if (isColumnFile(file)) {
                columns.add(file);
                continue;
            }
            baseFiles.add(file);
            if (file.firstRowId() == null) {
                legacyFile = file;
            } else {
                DataFileMeta previous = basesByFirstRowId.put(file.firstRowId(), file);
                if (previous != null) {
                    throw new IllegalStateException(
                            String.format(
                                    "Files %s and %s have the same first row id %s.",
                                    previous.fileName(), file.fileName(), file.firstRowId()));
                }
            }
        }

        baseFiles = Collections.unmodifiableList(baseFiles);
        if (columns.isEmpty()) {
            return new ColumnFileGroups(baseFiles, new ConcurrentHashMap<>());
        }

        if (legacyFile != null && !allowLegacyFiles) {
            throw new IllegalStateException(
                    String.format(
                            "File %s was written before data evolution was enabled and has no "
                                    + "alignment range, it cannot be read together with column files "
                                    + "in the same bucket. Run a full compaction of the bucket to "
                                    + "migrate it first.",
                            legacyFile.fileName()));
        }

        Map<String, List<DataFileMeta>> columnFiles = new ConcurrentHashMap<>();
        for (DataFileMeta column : columns) {
            DataFileMeta base =
                    column.firstRowId() == null ? null : basesByFirstRowId.get(column.firstRowId());
            if (base == null || base.rowCount() != column.rowCount()) {
                throw new IllegalStateException(
                        String.format(
                                "Column file %s (first row id %s, row count %s) has no base file "
                                        + "with the same alignment range.",
                                column.fileName(), column.firstRowId(), column.rowCount()));
            }
            columnFiles.computeIfAbsent(base.fileName(), k -> new ArrayList<>()).add(column);
        }
        return new ColumnFileGroups(baseFiles, columnFiles);
    }

    public List<DataFileMeta> baseFiles() {
        return baseFiles;
    }

    public List<DataFileMeta> columnFilesOf(DataFileMeta base) {
        List<DataFileMeta> files = columnFiles.get(base.fileName());
        return files == null ? Collections.emptyList() : files;
    }

    /**
     * Forgets the column files of a base file once it has left the merge tree. Must only be called
     * after the compaction result removing the base file is applied.
     */
    public void dropBaseFile(String baseFileName) {
        columnFiles.remove(baseFileName);
    }
}
