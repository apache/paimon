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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreScan;
import org.apache.flink.table.store.file.utils.BinPacking;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The {@code FileStoreSplitGenerator}'s task is to plan all files to be read and to split them into
 * a set of {@link FileStoreSourceSplit}.
 */
public class FileStoreSourceSplitGenerator {

    /**
     * The current Id as a mutable string representation. This covers more values than the integer
     * value range, so we should never overflow.
     */
    private final char[] currentId = "0000000000".toCharArray();

    public List<FileStoreSourceSplit> createSplits(FileStoreScan.Plan plan) {
        return createSplits(plan.groupByPartFiles());
    }

    public List<FileStoreSourceSplit> createSplits(
            Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupBy) {
        return groupBy.entrySet().stream()
                .flatMap(
                        pe ->
                                pe.getValue().entrySet().stream()
                                        .map(
                                                be ->
                                                        new FileStoreSourceSplit(
                                                                getNextId(),
                                                                pe.getKey(),
                                                                be.getKey(),
                                                                be.getValue())))
                .collect(Collectors.toList());
    }

    public List<FileStoreSourceSplit> createBinPackingSplits(
            FileStoreScan.Plan plan, long targetSplitSize) {
        Map<BinaryRowData, Map<Integer, List<DataFileMeta>>> groupByPart = plan.groupByPartFiles();

        List<FileStoreSourceSplit> splits = new ArrayList<>();
        for (Map.Entry<BinaryRowData, Map<Integer, List<DataFileMeta>>> part :
                groupByPart.entrySet()) {
            BinaryRowData partKey = part.getKey();

            Map<Integer, List<DataFileMeta>> buckets = part.getValue();
            for (Map.Entry<Integer, List<DataFileMeta>> bucket : buckets.entrySet()) {
                Integer bucketId = bucket.getKey();
                List<DataFileMeta> bucketFiles = bucket.getValue();

                for (List<DataFileMeta> binPackFiles :
                        binPackSplits(bucketFiles, targetSplitSize)) {
                    FileStoreSourceSplit split =
                            new FileStoreSourceSplit(getNextId(), partKey, bucketId, binPackFiles);
                    splits.add(split);
                }
            }
        }

        return splits;
    }

    private static List<List<DataFileMeta>> binPackSplits(
            List<DataFileMeta> bucketFiles, long targetSplitSize) {
        long openFileCost = 1024 * 1024L;
        int lookback = 0;
        Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);

        List<List<DataFileMeta>> packList = new ArrayList<>();
        for (List<DataFileMeta> part :
                new BinPacking.PackingIterable<>(
                        bucketFiles, targetSplitSize, lookback, weightFunc, true)) {
            packList.add(part);
        }

        return packList;
    }

    protected final String getNextId() {
        // because we just increment numbers, we increment the char representation directly,
        // rather than incrementing an integer and converting it to a string representation
        // every time again (requires quite some expensive conversion logic).
        incrementCharArrayByOne(currentId, currentId.length - 1);
        return new String(currentId);
    }

    private static void incrementCharArrayByOne(char[] array, int pos) {
        if (pos < 0) {
            throw new RuntimeException("Produce too many splits.");
        }

        char c = array[pos];
        c++;

        if (c > '9') {
            c = '0';
            incrementCharArrayByOne(array, pos - 1);
        }
        array[pos] = c;
    }
}
