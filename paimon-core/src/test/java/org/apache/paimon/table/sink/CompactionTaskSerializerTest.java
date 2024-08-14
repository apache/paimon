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

import org.apache.paimon.append.MultiTableUnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.catalog.Identifier;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.paimon.manifest.ManifestCommittableSerializerTest.randomNewFilesIncrement;
import static org.apache.paimon.mergetree.compact.MergeTreeCompactManagerTest.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompactionTaskSerializer} and {@link MultiTableCompactionTaskSerializer}. */
public class CompactionTaskSerializerTest {

    @Test
    public void testCompactionTaskSerializer() throws IOException {
        CompactionTaskSerializer serializer = new CompactionTaskSerializer();
        UnawareAppendCompactionTask task =
                new UnawareAppendCompactionTask(row(0), randomNewFilesIncrement().newFiles());

        byte[] bytes = serializer.serialize(task);
        UnawareAppendCompactionTask task1 = serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(task).isEqualTo(task1);
    }

    @Test
    public void testMultiTableCompactionTaskSerializer() throws IOException {
        MultiTableCompactionTaskSerializer serializer = new MultiTableCompactionTaskSerializer();
        MultiTableUnawareAppendCompactionTask task =
                new MultiTableUnawareAppendCompactionTask(
                        row(0),
                        randomNewFilesIncrement().newFiles(),
                        Identifier.create("db", "table"));

        byte[] bytes = serializer.serialize(task);
        MultiTableUnawareAppendCompactionTask task1 =
                serializer.deserialize(serializer.getVersion(), bytes);
        assertThat(task).isEqualTo(task1);
    }
}
