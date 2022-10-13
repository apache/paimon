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

package org.apache.flink.table.store.table.sink;

import org.apache.flink.table.store.file.io.CompactIncrement;
import org.apache.flink.table.store.file.io.NewFilesIncrement;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest.randomCompactIncrement;
import static org.apache.flink.table.store.file.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.flink.table.store.file.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileCommittableSerializer}. */
public class FileCommittableSerializerTest {

    @Test
    public void test() throws IOException {
        FileCommittableSerializer serializer = new FileCommittableSerializer();
        NewFilesIncrement newFilesIncrement = randomNewFilesIncrement();
        CompactIncrement compactIncrement = randomCompactIncrement();
        FileCommittable committable =
                new FileCommittable(row(0), 1, newFilesIncrement, compactIncrement);
        FileCommittable newCommittable =
                serializer.deserialize(2, serializer.serialize(committable));
        assertThat(newCommittable).isEqualTo(committable);
    }
}
