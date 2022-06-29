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

package org.apache.flink.table.store.file.data;

import org.apache.flink.table.store.file.compact.CompactStrategy;
import org.apache.flink.table.store.file.compact.CompactUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Compact strategy to pick small files for {@link
 * org.apache.flink.table.store.file.AppendOnlyFileStore}.
 */
public class AppendOnlyCompactStrategy
        implements CompactStrategy<List<DataFileMeta>, List<CompactUnit>> {

    private final long targetFileSize;

    public AppendOnlyCompactStrategy(long targetFileSize) {
        this.targetFileSize = targetFileSize;
    }

    /**
     * Pick compact units from meta files sorted by sequence number.
     *
     * <ul>
     *   <li>adjacent small files will be grouped to one compact unit until the sum of file size
     *       exceeds targetFileSize. Therefore, files within one unit always have continuous
     *       sequence number.
     *   <li>in order to eliminate the small file at best efforts, for a single small file, the
     *       strategy will try to pick adjacent previous file along with the current small file.
     * </ul>
     */
    @Override
    public Optional<List<CompactUnit>> pick(List<DataFileMeta> newFiles) {
        List<CompactUnit> units = new ArrayList<>();
        List<DataFileMeta> unitFiles = new ArrayList<>();
        if (newFiles.size() < 2) {
            return Optional.empty();
        }
        long totalFileSize = 0L;
        for (int i = 0; i < newFiles.size(); i++) {
            DataFileMeta file = newFiles.get(i);
            if (file.fileSize() < targetFileSize) {
                unitFiles.add(file);
                totalFileSize += file.fileSize();
                if (totalFileSize >= targetFileSize) {
                    totalFileSize = generateUnit(units, unitFiles);
                }
                if (i < newFiles.size() - 1) {
                    DataFileMeta nextFile = newFiles.get(i + 1);
                    if (nextFile.fileSize() >= targetFileSize) {
                        if (unitFiles.size() == 1) {
                            // rewrite previous collected small files along with the next large file
                            unitFiles.add(nextFile);
                        }
                        totalFileSize = generateUnit(units, unitFiles);
                    }
                }
            }
        }
        if (!unitFiles.isEmpty()) {
            // collect at least two small files
            if (unitFiles.size() > 1) {
                generateUnit(units, unitFiles);
            } else {
                DataFileMeta single = unitFiles.get(0);
                // find previous file to form a unit
                int idx = newFiles.indexOf(single);
                DataFileMeta previous = newFiles.get(idx - 1);
                if (previous.fileSize() < targetFileSize) {
                    // pad single small file to the previous unit
                    CompactUnit previousUnit = units.remove(units.size() - 1);
                    List<DataFileMeta> files = new ArrayList<>(previousUnit.files());
                    files.add(single);
                    units.add(CompactUnit.fromFiles(DataFileMeta.DUMMY_LEVEL, files));
                } else {
                    units.add(
                            CompactUnit.fromFiles(
                                    DataFileMeta.DUMMY_LEVEL, Arrays.asList(previous, single)));
                }
            }
        }

        return Optional.of(units);
    }

    private long generateUnit(List<CompactUnit> units, List<DataFileMeta> files) {
        units.add(CompactUnit.fromFiles(DataFileMeta.DUMMY_LEVEL, new ArrayList<>(files)));
        files.clear();
        return 0L;
    }
}
