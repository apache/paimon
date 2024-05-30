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

package org.apache.paimon.io;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.stats.StatsTestUtils;

import java.util.Collections;

/** Utils for {@link DataFileMeta}. */
public class DataFileTestUtils {

    public static DataFileMeta fromMinMax(String name, int minKey, int maxKey) {
        return newFile(name, 0, minKey, maxKey, 0);
    }

    public static DataFileMeta newFile(int level, int minKey, int maxKey, long maxSequence) {
        return newFile("", level, minKey, maxKey, maxSequence);
    }

    public static DataFileMeta newFile(long minSeq, long maxSeq) {
        return new DataFileMeta(
                "",
                maxSeq - minSeq + 1,
                0L,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                DataFileMeta.EMPTY_KEY_STATS,
                null,
                minSeq,
                maxSeq,
                0L,
                DataFileMeta.DUMMY_LEVEL,
                Collections.emptyList(),
                Timestamp.fromEpochMillis(100),
                maxSeq - minSeq + 1,
                null);
    }

    public static DataFileMeta newFile() {
        return new DataFileMeta(
                "",
                0,
                0,
                DataFileMeta.EMPTY_MIN_KEY,
                DataFileMeta.EMPTY_MAX_KEY,
                StatsTestUtils.newEmptyTableStats(),
                StatsTestUtils.newEmptyTableStats(),
                0,
                0,
                0,
                0,
                0L,
                null);
    }

    public static DataFileMeta newFile(
            String name, int level, int minKey, int maxKey, long maxSequence) {
        return newFile(name, level, minKey, maxKey, maxSequence, 0L);
    }

    public static DataFileMeta newFile(
            String name, int level, int minKey, int maxKey, long maxSequence, Long deleteRowCount) {
        return new DataFileMeta(
                name,
                maxKey - minKey + 1,
                maxKey - minKey + 1,
                row(minKey),
                row(maxKey),
                null,
                null,
                0,
                maxSequence,
                0,
                level,
                deleteRowCount,
                null);
    }

    public static BinaryRow row(int i) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, i);
        writer.complete();
        return row;
    }

    public static BinaryRow row(int... values) {
        BinaryRow row = new BinaryRow(values.length);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        for (int i = 0; i < values.length; i++) {
            writer.writeInt(i, values[i]);
        }
        writer.complete();
        return row;
    }
}
