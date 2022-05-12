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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.runtime.operators.sink.TestSink;
import org.apache.flink.table.store.file.mergetree.Increment;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest.randomIncrement;
import static org.apache.flink.table.store.file.mergetree.compact.CompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CommittableSerializer}. */
public class CommittableSerializerTest {

    private final FileCommittableSerializer fileSerializer = new FileCommittableSerializer();

    private final CommittableSerializer serializer =
            new CommittableSerializer(
                    fileSerializer,
                    (SimpleVersionedSerializer) TestSink.StringCommittableSerializer.INSTANCE);

    @Test
    public void testFile() throws IOException {
        Increment increment = randomIncrement();
        FileCommittable committable = new FileCommittable(row(0), 1, increment);
        FileCommittable newCommittable =
                (FileCommittable)
                        serializer
                                .deserialize(
                                        1,
                                        serializer.serialize(
                                                new Committable(
                                                        Committable.Kind.FILE, committable)))
                                .wrappedCommittable();
        assertThat(newCommittable).isEqualTo(committable);
    }

    @Test
    public void testLogOffset() throws IOException {
        LogOffsetCommittable committable = new LogOffsetCommittable(2, 3);
        LogOffsetCommittable newCommittable =
                (LogOffsetCommittable)
                        serializer
                                .deserialize(
                                        1,
                                        serializer.serialize(
                                                new Committable(
                                                        Committable.Kind.LOG_OFFSET, committable)))
                                .wrappedCommittable();
        assertThat(newCommittable).isEqualTo(committable);
    }

    @Test
    public void testLog() throws IOException {
        String log = "random_string";
        String newCommittable =
                (String)
                        serializer
                                .deserialize(
                                        1,
                                        serializer.serialize(
                                                new Committable(Committable.Kind.LOG, log)))
                                .wrappedCommittable();
        assertThat(newCommittable).isEqualTo(log);
    }
}
