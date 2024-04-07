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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/** Merge tree implementation of {@link SplitGenerator}. */
public class MergeTreeSplitGenerator implements SplitGenerator {

    private final Comparator<InternalRow> keyComparator;

    private final long targetSplitSize;

    private final long openFileCost;

    private final boolean deletionVectorsEnabled;

    private final MergeEngine mergeEngine;

    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }

    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        boolean rawConvertible =
                files.stream().allMatch(file -> file.level() != 0 && withoutDeleteRow(file));
        boolean oneLevel =
                files.stream().map(DataFileMeta::level).collect(Collectors.toSet()).size() == 1;

        if (rawConvertible && (deletionVectorsEnabled || mergeEngine == FIRST_ROW || oneLevel)) {
            Function<DataFileMeta, Long> weightFunc =
                    file -> Math.max(file.fileSize(), openFileCost);
            return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                    .map(SplitGroup::rawConvertibleGroup)
                    .collect(Collectors.toList());
        }

        /*
         * The generator aims to parallel the scan execution by slicing the files of each bucket
         * into multiple splits. The generation has one constraint: files with intersected key
         * ranges (within one section) must go to the same split. Therefore, the files are first to go
         * through the interval partition algorithm to generate sections and then through the
         * OrderedPack algorithm. Note that the item to be packed here is each section, the capacity
         * is denoted as the targetSplitSize, and the final number of the bins is the number of
         * splits generated.
         *
         * For instance, there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
         * with targetSplitSize 128M. After interval partition, there are four sections:
         * - section1: [1, 2]
         * - section2: [3, 4]
         * - section3: [5, 180], [5, 190]
         * - section4: [200, 600], [210, 700]
         *
         * After OrderedPack, section1 and section2 will be put into one bin (split), so the final result will be:
         * - split1: [1, 2] [3, 4]
         * - split2: [5, 180] [5,190]
         * - split3: [200, 600] [210, 700]
         */
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        return packSplits(sections).stream()
                .map(
                        f ->
                                f.size() == 1 && withoutDeleteRow(f.get(0))
                                        ? SplitGroup.rawConvertibleGroup(f)
                                        : SplitGroup.nonRawConvertibleGroup(f))
                .collect(Collectors.toList());
    }

    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // We don't split streaming scan files
        return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
    }

    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
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

    private boolean withoutDeleteRow(DataFileMeta dataFileMeta) {
        return dataFileMeta.deleteRowCount().map(count -> count == 0L).orElse(false);
    }
}
