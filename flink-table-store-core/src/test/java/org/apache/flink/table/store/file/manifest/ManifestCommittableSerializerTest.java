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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.mergetree.Increment;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.apache.flink.table.store.file.stats.StatsTestUtils.newTableStats;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ManifestCommittableSerializer}. */
public class ManifestCommittableSerializerTest {

    private static final AtomicInteger ID = new AtomicInteger();

    @Test
    public void testCommittableSerDe() throws IOException {
        ManifestCommittableSerializer serializer = serializer();
        ManifestCommittable committable = create();
        byte[] serialized = serializer.serialize(committable);
        assertThat(serializer.deserialize(1, serialized)).isEqualTo(committable);
    }

    public static ManifestCommittableSerializer serializer() {
        return new ManifestCommittableSerializer();
    }

    public static ManifestCommittable create() {
        ManifestCommittable committable =
                new ManifestCommittable(String.valueOf(new Random().nextLong()));
        addAndAssert(committable, row(0), 0);
        addAndAssert(committable, row(0), 1);
        addAndAssert(committable, row(1), 0);
        addAndAssert(committable, row(1), 1);
        return committable;
    }

    private static void addAndAssert(
            ManifestCommittable committable, BinaryRowData partition, int bucket) {
        Increment increment = randomIncrement();
        committable.addFileCommittable(partition, bucket, increment);
        assertThat(committable.newFiles().get(partition).get(bucket))
                .isEqualTo(increment.newFiles());
        assertThat(committable.compactBefore().get(partition).get(bucket))
                .isEqualTo(increment.compactBefore());
        assertThat(committable.compactAfter().get(partition).get(bucket))
                .isEqualTo(increment.compactAfter());

        if (!committable.logOffsets().containsKey(bucket)) {
            int offset = ID.incrementAndGet();
            committable.addLogOffset(bucket, offset);
            assertThat(committable.logOffsets().get(bucket)).isEqualTo(offset);
        }
    }

    public static Increment randomIncrement() {
        return new Increment(
                Arrays.asList(newFile(ID.incrementAndGet(), 0), newFile(ID.incrementAndGet(), 0)),
                Arrays.asList(newFile(ID.incrementAndGet(), 0), newFile(ID.incrementAndGet(), 0)),
                Arrays.asList(newFile(ID.incrementAndGet(), 0), newFile(ID.incrementAndGet(), 0)));
    }

    public static DataFileMeta newFile(int name, int level) {
        return new DataFileMeta(
                String.valueOf(name),
                0,
                1,
                row(0),
                row(0),
                newTableStats(0, 1),
                newTableStats(0, 1),
                0,
                1,
                0,
                level);
    }
}
