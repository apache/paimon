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

package org.apache.paimon.flink.source;

import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import java.util.List;
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

    public List<FileStoreSourceSplit> createSplits(TableScan.Plan plan) {
        return plan.splits().stream()
                .map(s -> new FileStoreSourceSplit(getNextId(), s))
                .collect(Collectors.toList());
    }

    public List<FileStoreSourceSplit> createSplits(List<Split> splits) {
        return splits.stream()
                .map(s -> new FileStoreSourceSplit(getNextId(), s))
                .collect(Collectors.toList());
    }

    public FileStoreSourceSplit createSplit(Split split) {
        return new FileStoreSourceSplit(getNextId(), split);
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
