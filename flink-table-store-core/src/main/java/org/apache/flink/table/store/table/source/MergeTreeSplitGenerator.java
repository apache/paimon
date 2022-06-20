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

package org.apache.flink.table.store.table.source;

import org.apache.flink.connector.file.table.BinPacking;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Merge tree implementation of {@link SplitGenerator}. */
public class MergeTreeSplitGenerator implements SplitGenerator {

    private final Comparator<RowData> keyComparator;

    private final long targetSplitSize;

    private final long openFileCost;

    public MergeTreeSplitGenerator(
            Comparator<RowData> keyComparator, long targetSplitSize, long openFileCost) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
    }

    @Override
    public List<List<DataFileMeta>> split(List<DataFileMeta> files) {
        /*
         * Suppose now there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
         * Files without intersection are not related, we do not need to put all files into one
         * split, we can slice into multiple splits, multiple parallelism execution is faster.
         * Nor can we slice too fine, we should make each split as large as possible with 128 MB,
         * so use BinPack to slice, the final result will be:
         *
         * - split1: [1, 2] [3, 4]
         * - split2: [5, 180] [5, 190]
         * - split3: [200, 600] [210, 700]
         */
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        return binPackSplits(sections);
    }

    private List<List<DataFileMeta>> binPackSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.pack(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }
}
