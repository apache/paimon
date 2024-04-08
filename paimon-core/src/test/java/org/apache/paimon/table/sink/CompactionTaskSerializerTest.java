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

package org.apache.paimon.table.sink;

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionTaskSerializer}. */
public class CompactionTaskSerializerTest {

    @Test
    public void testCompactionTaskSerializer() throws IOException {
        {
            CompactionTaskSerializer serializer = new CompactionTaskSerializer();
            AppendOnlyCompactionTask task =
                    new AppendOnlyCompactionTask(row(0), randomNewFilesIncrement().newFiles());

            byte[] bytes = serializer.serialize(task);
            AppendOnlyCompactionTask task1 = serializer.deserialize(serializer.getVersion(), bytes);
            assertThat(task).isEqualTo(task1);
        }

        {
            CompactionTaskSerializer serializer = new CompactionTaskSerializer();
            AppendOnlyCompactionTask task =
                    new AppendOnlyCompactionTask(
                            row(0),
                            randomNewFilesIncrement().newFiles(),
                            Identifier.create("db", "table"));

            byte[] bytes = serializer.serialize(task);
            AppendOnlyCompactionTask task1 = serializer.deserialize(serializer.getVersion(), bytes);
            assertThat(task).isEqualTo(task1);
        }
    }
}
