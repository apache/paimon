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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.WrappedManifestCommittable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.assertj.core.api.Assertions.assertThat;

class WrappedManifestCommittableSerializerTest {

    private static final AtomicInteger ID = new AtomicInteger();
    private static final int VERSION = WrappedManifestCommittableSerializer.CURRENT_VERSION;

    @Test
    public void testCommittableSerDe() throws IOException {
        WrappedManifestCommittableSerializer serializer = serializer();
        ManifestCommittable committable1 = createManifestCommittable();
        ManifestCommittable committable2 = createManifestCommittable();
        WrappedManifestCommittable wrappedManifestCommittable =
                new WrappedManifestCommittable(-1, -1);
        wrappedManifestCommittable.putManifestCommittable(
                Identifier.create("db", "table1"), committable1);
        wrappedManifestCommittable.putManifestCommittable(
                Identifier.create("db", "table2"), committable2);
        byte[] serialized = serializer.serialize(wrappedManifestCommittable);
        WrappedManifestCommittable deserialize = serializer.deserialize(VERSION, serialized);
        Map<Identifier, ManifestCommittable> manifestCommittables =
                deserialize.manifestCommittables();

        assertThat(manifestCommittables.size()).isEqualTo(2);
        assertThat(deserialize).isEqualTo(wrappedManifestCommittable);
    }

    public static WrappedManifestCommittableSerializer serializer() {
        return new WrappedManifestCommittableSerializer();
    }

    public static ManifestCommittable createManifestCommittable() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        ManifestCommittable committable =
                rnd.nextBoolean()
                        ? new ManifestCommittable(rnd.nextLong(), rnd.nextLong())
                        : new ManifestCommittable(rnd.nextLong(), null);
        addFileCommittables(committable, row(0), 0);
        addFileCommittables(committable, row(0), 1);
        addFileCommittables(committable, row(1), 0);
        addFileCommittables(committable, row(1), 1);
        return committable;
    }

    public static void addFileCommittables(
            ManifestCommittable committable, BinaryRow partition, int bucket) {
        List<CommitMessage> commitMessages = new ArrayList<>();
        int length = ThreadLocalRandom.current().nextInt(10) + 1;
        for (int i = 0; i < length; i++) {
            DataIncrement dataIncrement = randomNewFilesIncrement();
            CompactIncrement compactIncrement = randomCompactIncrement();
            CommitMessage commitMessage =
                    new CommitMessageImpl(partition, bucket, dataIncrement, compactIncrement);
            commitMessages.add(commitMessage);
            committable.addFileCommittable(commitMessage);
        }

        if (!committable.logOffsets().containsKey(bucket)) {
            int offset = ID.incrementAndGet();
            committable.addLogOffset(bucket, offset, false);
            assertThat(committable.logOffsets().get(bucket)).isEqualTo(offset);
        }
    }
}
