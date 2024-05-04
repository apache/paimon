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
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.Arrays;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

class MultiTableCommittableSerializerTest {

    private final CommitMessageSerializer fileSerializer = new CommitMessageSerializer();

    private final MultiTableCommittableSerializer serializer =
            new MultiTableCommittableSerializer(fileSerializer);

    @Test
    public void testDeserialize() {
        DataIncrement dataIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        CommitMessage commitMessage =
                new CommitMessageImpl(row(0), 1, dataIncrement, compactIncrement);
        Committable committable = new Committable(9, Committable.Kind.FILE, commitMessage);

        Arrays.asList(Tuple2.of("测试数据库", "用户信息表"), Tuple2.of("database", "table"))
                .forEach(
                        tuple2 -> {
                            String database = tuple2.f0;
                            String table = tuple2.f1;
                            MultiTableCommittable multiTableCommittable =
                                    MultiTableCommittable.fromCommittable(
                                            Identifier.create(database, table), committable);
                            MultiTableCommittable deserializeCommittable;
                            try {
                                deserializeCommittable =
                                        serializer.deserialize(
                                                2, serializer.serialize(multiTableCommittable));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            assertThat(deserializeCommittable)
                                    .isInstanceOf(MultiTableCommittable.class);

                            assertThat(deserializeCommittable.getDatabase()).isEqualTo(database);
                            assertThat(deserializeCommittable.getTable()).isEqualTo(table);
                        });
    }

    @Test
    public void testSerialize() {
        DataIncrement newFilesIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        CommitMessage commitMessage =
                new CommitMessageImpl(row(0), 1, newFilesIncrement, compactIncrement);
        Committable committable = new Committable(9, Committable.Kind.FILE, commitMessage);

        Arrays.asList(Tuple2.of("测试数据库", "用户信息表"), Tuple2.of("database", "table"))
                .forEach(
                        tuple2 -> {
                            String database = tuple2.f0;
                            String table = tuple2.f1;

                            MultiTableCommittable multiTableCommittable =
                                    MultiTableCommittable.fromCommittable(
                                            Identifier.create(database, table), committable);

                            byte[] serializedData = null;
                            try {
                                serializedData = serializer.serialize(multiTableCommittable);
                            } catch (BufferOverflowException e) {
                                e.printStackTrace();
                                assert false : "Should not throw BufferOverflowException";
                            } catch (IOException e) {
                                e.printStackTrace();
                                assert false : "IOException occurred";
                            }

                            assertThat(serializedData).isNotNull();
                        });
    }
}
