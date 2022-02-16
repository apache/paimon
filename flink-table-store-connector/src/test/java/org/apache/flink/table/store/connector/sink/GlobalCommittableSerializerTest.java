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
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link GlobalCommittableSerializer}. */
public class GlobalCommittableSerializerTest {

    @Test
    public void test() throws IOException {
        List<String> logs = Arrays.asList("1", "2");
        GlobalCommittable<String> committable =
                new GlobalCommittable<>(logs, ManifestCommittableSerializerTest.create());
        GlobalCommittableSerializer<String> serializer =
                new GlobalCommittableSerializer<>(
                        new TestStringSerializer(), ManifestCommittableSerializerTest.serializer());
        byte[] serialized = serializer.serialize(committable);
        GlobalCommittable<String> deser = serializer.deserialize(1, serialized);
        assertThat(deser.logCommittables()).isEqualTo(committable.logCommittables());
        assertThat(deser.fileCommittable()).isEqualTo(committable.fileCommittable());
    }

    private static final class TestStringSerializer implements SimpleVersionedSerializer<String> {

        private static final int VERSION = 1073741823;

        private TestStringSerializer() {}

        public int getVersion() {
            return VERSION;
        }

        public byte[] serialize(String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }

        public String deserialize(int version, byte[] serialized) {
            assertThat(version).isEqualTo(VERSION);
            return new String(serialized, StandardCharsets.UTF_8);
        }
    }
}
