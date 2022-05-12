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

import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.SortedRun;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This strategy aims to eliminate the intersected key range in different sorted runs, and merge
 * small files with the best effort. The pick candidate based on sorted runs which have been
 * partitioned by {@link IntervalPartition} algorithm.
 */
public class ManualTriggeredCompaction implements CompactStrategy<SortedRun> {

    private final long targetFileSize;

    public ManualTriggeredCompaction(long targetFileSize) {
        this.targetFileSize = targetFileSize;
    }

    @Override
    public Optional<CompactUnit> pick(int numLevels, List<SortedRun> runs) {
        if (runs.size() == 1) {
            // for section which does not contain overlapped keys, try to merge small files
            List<DataFileMeta> files = runs.get(0).files();
            long ctr = files.stream().filter(file -> file.fileSize() < targetFileSize).count();
            if (ctr > 1) {
                return Optional.of(CompactUnit.fromFiles(numLevels - 1, files));
            }
        } else if (runs.size() > 1) {
            // for section which contains overlapped keys, pick them all
            return Optional.of(
                    CompactUnit.fromFiles(
                            numLevels - 1,
                            runs.stream()
                                    .map(SortedRun::files)
                                    .flatMap(Collection::stream)
                                    .collect(Collectors.toList())));
        }

        return Optional.empty();
    }
}
