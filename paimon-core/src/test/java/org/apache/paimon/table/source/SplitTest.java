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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DataSplit}. */
public class SplitTest {

    @Test
    public void testSerializer() throws IOException {
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().build();
        DataFileTestDataGenerator.Data data = gen.next();
        List<DataFileMeta> files = new ArrayList<>();
        for (int i = 0; i < ThreadLocalRandom.current().nextInt(10); i++) {
            files.add(gen.next().meta);
        }
        DataSplit split =
                DataSplit.builder()
                        .withSnapshot(ThreadLocalRandom.current().nextLong(100))
                        .withPartition(data.partition)
                        .withBucket(data.bucket)
                        .withDataFiles(files)
                        .withBucketPath("my path")
                        .withDefaultFormat("orc")
                        .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        split.serialize(new DataOutputViewStreamWrapper(out));

        DataSplit newSplit = DataSplit.deserialize(new DataInputDeserializer(out.toByteArray()));
        assertThat(newSplit).isEqualTo(split);
    }
}
