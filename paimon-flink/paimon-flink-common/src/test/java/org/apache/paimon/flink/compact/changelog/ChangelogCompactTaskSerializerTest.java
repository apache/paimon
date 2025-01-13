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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.apache.paimon.stats.StatsTestUtils.newSimpleStats;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ChangelogCompactTaskSerializer}. */
public class ChangelogCompactTaskSerializerTest {
    private final ChangelogCompactTaskSerializer serializer = new ChangelogCompactTaskSerializer();

    @Test
    public void testSerializer() throws Exception {
        BinaryRow partition = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(partition);
        writer.writeInt(0, 0);
        writer.complete();

        ChangelogCompactTask task =
                new ChangelogCompactTask(
                        1L,
                        partition,
                        new HashMap<Integer, List<DataFileMeta>>() {
                            {
                                put(0, newFiles(20));
                                put(1, newFiles(20));
                            }
                        },
                        new HashMap<Integer, List<DataFileMeta>>() {
                            {
                                put(0, newFiles(10));
                                put(1, newFiles(10));
                            }
                        });
        ChangelogCompactTask serializeTask = serializer.deserialize(1, serializer.serialize(task));
        assertThat(task).isEqualTo(serializeTask);
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
                FileSource.APPEND,
                null);
    }
}
