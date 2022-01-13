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

package org.apache.flink.table.store.file.mergetree.compact;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/** Algorithm to partition several sst files into the minimum number of {@link SortedRun}s. */
public class IntervalPartition {

    private final List<SstFileMeta> files;
    private final Comparator<RowData> keyComparator;

    public IntervalPartition(List<SstFileMeta> inputFiles, Comparator<RowData> keyComparator) {
        this.files = new ArrayList<>(inputFiles);
        this.files.sort(
                (o1, o2) -> {
                    int leftResult = keyComparator.compare(o1.minKey(), o2.minKey());
                    return leftResult == 0
                            ? keyComparator.compare(o1.maxKey(), o2.maxKey())
                            : leftResult;
                });
        this.keyComparator = keyComparator;
    }

    /**
     * Returns a two-dimensional list of {@link SortedRun}s.
     *
     * <p>The elements of the outer list are sections. Key intervals between sections do not
     * overlap. This extra layer is to minimize the number of {@link SortedRun}s dealt at the same
     * time.
     *
     * <p>The elements of the inner list are {@link SortedRun}s within a section.
     *
     * <p>Users are expected to use the results by this way:
     *
     * <pre>{@code
     * for (List<SortedRun> section : algorithm.partition()) {
     *     // do some merge sorting within section
     * }
     * }</pre>
     */
    public List<List<SortedRun>> partition() {
        List<List<SortedRun>> result = new ArrayList<>();
        List<SstFileMeta> section = new ArrayList<>();
        BinaryRowData bound = null;

        for (SstFileMeta meta : files) {
            if (!section.isEmpty() && keyComparator.compare(meta.minKey(), bound) > 0) {
                // larger than current right bound, conclude current section and create a new one
                result.add(partition(section));
                section.clear();
                bound = null;
            }
            section.add(meta);
            if (bound == null || keyComparator.compare(meta.maxKey(), bound) > 0) {
                // update right bound
                bound = meta.maxKey();
            }
        }
        if (!section.isEmpty()) {
            // conclude last section
            result.add(partition(section));
        }

        return result;
    }

    private List<SortedRun> partition(List<SstFileMeta> metas) {
        PriorityQueue<List<SstFileMeta>> queue =
                new PriorityQueue<>(
                        (o1, o2) ->
                                // sort by max key of the last sst file
                                keyComparator.compare(
                                        o1.get(o1.size() - 1).maxKey(),
                                        o2.get(o2.size() - 1).maxKey()));
        // create the initial partition
        List<SstFileMeta> firstRun = new ArrayList<>();
        firstRun.add(metas.get(0));
        queue.add(firstRun);

        for (int i = 1; i < metas.size(); i++) {
            SstFileMeta meta = metas.get(i);
            // any file list whose max key < meta.minKey() is sufficient,
            // for convenience we pick the smallest
            List<SstFileMeta> top = queue.poll();
            if (keyComparator.compare(meta.minKey(), top.get(top.size() - 1).maxKey()) > 0) {
                // append current file to an existing partition
                top.add(meta);
            } else {
                // create a new partition
                List<SstFileMeta> newRun = new ArrayList<>();
                newRun.add(meta);
                queue.add(newRun);
            }
            queue.add(top);
        }

        // order between partitions does not matter
        return queue.stream().map(SortedRun::fromSorted).collect(Collectors.toList());
    }
}
