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

package org.apache.paimon.flink.sink;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.sink.CompactionTaskSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.apache.paimon.stats.StatsTestUtils.newSimpleStats;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CompactionTaskSimpleSerializer}. */
public class CompactionTaskSimpleSerializerTest {

    private final CompactionTaskSerializer compactionTaskSerializer =
            new CompactionTaskSerializer();

    private final CompactionTaskSimpleSerializer serializer =
            new CompactionTaskSimpleSerializer(compactionTaskSerializer);

    private final BinaryRow partition = BinaryRow.EMPTY_ROW;

    @Test
    public void testSerializer() throws IOException {

        AppendOnlyCompactionTask task1 = new AppendOnlyCompactionTask(partition, newFiles(20));
        AppendOnlyCompactionTask task2 = serializer.deserialize(2, serializer.serialize(task1));

        assertThat(task1).isEqualTo(task2);
    }

    private List<DataFileMeta> newFiles(int num) {
        List<DataFileMeta> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            list.add(newFile());
        }
        return list;
    }

    private DataFileMeta newFile() {
        return new DataFileMeta(
                UUID.randomUUID().toString(),
                0,
                1,
                row(0),
                row(0),
                newSimpleStats(0, 1),
                newSimpleStats(0, 1),
                0,
                1,
                0,
                0,
                0L,
                null,
                FileSource.APPEND);
    }
}
