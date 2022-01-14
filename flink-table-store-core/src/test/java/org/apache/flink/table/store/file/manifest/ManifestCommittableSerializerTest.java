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
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMetaSerializer;
import org.apache.flink.table.store.file.stats.FieldStats;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ManifestCommittableSerializer}. */
public class ManifestCommittableSerializerTest {

    private final AtomicInteger id = new AtomicInteger();

    @Test
    public void testCommittableSerDe() throws IOException {
        SstFileMetaSerializer sstSerializer =
                new SstFileMetaSerializer(RowType.of(new IntType()), RowType.of(new IntType()));
        ManifestCommittableSerializer serializer =
                new ManifestCommittableSerializer(RowType.of(new IntType()), sstSerializer);
        ManifestCommittable committable = new ManifestCommittable();
        addAndAssert(committable, row(0), 0);
        addAndAssert(committable, row(0), 1);
        addAndAssert(committable, row(1), 0);
        addAndAssert(committable, row(1), 1);
        byte[] serialized = serializer.serialize(committable);
        assertThat(serializer.deserialize(1, serialized)).isEqualTo(committable);
    }

    private void addAndAssert(
            ManifestCommittable committable, BinaryRowData partition, int bucket) {
        Increment increment = newIncrement();
        committable.add(partition, bucket, increment);
        assertThat(committable.newFiles().get(partition).get(bucket))
                .isEqualTo(increment.newFiles());
        assertThat(committable.compactBefore().get(partition).get(bucket))
                .isEqualTo(increment.compactBefore());
        assertThat(committable.compactAfter().get(partition).get(bucket))
                .isEqualTo(increment.compactAfter());
    }

    private Increment newIncrement() {
        return new Increment(
                Arrays.asList(newFile(id.incrementAndGet(), 0), newFile(id.incrementAndGet(), 0)),
                Arrays.asList(newFile(id.incrementAndGet(), 0), newFile(id.incrementAndGet(), 0)),
                Arrays.asList(newFile(id.incrementAndGet(), 0), newFile(id.incrementAndGet(), 0)));
    }

    public static SstFileMeta newFile(int name, int level) {
        FieldStats[] stats = new FieldStats[] {new FieldStats(0, 1, 0)};
        return new SstFileMeta(String.valueOf(name), 0, 1, row(0), row(0), stats, 0, 1, level);
    }
}
