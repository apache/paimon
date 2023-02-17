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

import org.apache.flink.table.store.file.io.CompactIncrement;
import org.apache.flink.table.store.file.io.NewFilesIncrement;
import org.apache.flink.table.store.table.sink.FileCommittable;
import org.apache.flink.table.store.table.sink.FileCommittableSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link CommittableSerializer}. */
public class CommittableSerializerTest {

    private final FileCommittableSerializer fileSerializer = new FileCommittableSerializer();

    private final CommittableSerializer serializer = new CommittableSerializer(fileSerializer);

    @Test
    public void testFile() throws IOException {
        NewFilesIncrement newFilesIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        FileCommittable committable =
                new FileCommittable(row(0), 1, newFilesIncrement, compactIncrement);
        FileCommittable newCommittable =
                (FileCommittable)
                        serializer
                                .deserialize(
                                        2,
                                        serializer.serialize(
                                                new Committable(
                                                        9, Committable.Kind.FILE, committable)))
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
                                        2,
                                        serializer.serialize(
                                                new Committable(
                                                        8,
                                                        Committable.Kind.LOG_OFFSET,
                                                        committable)))
                                .wrappedCommittable();
        assertThat(newCommittable).isEqualTo(committable);
    }
}
