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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileTestDataGenerator;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonInputSplit}. */
public class PaimonInputSplitTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testWriteAndRead() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        DataFileTestDataGenerator gen = DataFileTestDataGenerator.builder().numBuckets(1).build();
        List<DataFileTestDataGenerator.Data> generated = new ArrayList<>();
        for (int i = random.nextInt(100) + 1; i > 0; i--) {
            generated.add(gen.next());
        }

        BinaryRow wantedPartition = generated.get(0).partition;
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(ThreadLocalRandom.current().nextLong(100))
                        .withPartition(wantedPartition)
                        .withBucket(0)
                        .withDataFiles(
                                generated.stream()
                                        .filter(d -> d.partition.equals(wantedPartition))
                                        .map(d -> d.meta)
                                        .collect(Collectors.toList()))
                        .build();
        PaimonInputSplit split = new PaimonInputSplit(tempDir.toString(), dataSplit);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos);
        split.write(output);
        byte[] bytes = baos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream input = new DataInputStream(bais);
        PaimonInputSplit actual = new PaimonInputSplit();
        actual.readFields(input);
        assertThat(actual).isEqualTo(split);
    }
}
