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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RowDataChannelComputer}. */
public class RowDataChannelComputerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSchemaWithPartition() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"pt", "k", "v"});

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                Collections.singletonMap("bucket", "1"),
                                ""));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numInputs = random.nextInt(1000) + 1;
        List<InternalRow> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            input.add(
                    GenericRow.of(random.nextInt(10) + 1, random.nextLong(), random.nextDouble()));
        }

        testImpl(schema, input);
    }

    @Test
    public void testSchemaNoPartition() throws Exception {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"k", "v"});

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                Collections.singletonMap("bucket", "1"),
                                ""));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numInputs = random.nextInt(1000) + 1;
        List<InternalRow> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            input.add(GenericRow.of(random.nextLong(), random.nextDouble()));
        }

        testImpl(schema, input);
    }

    private void testImpl(TableSchema schema, List<InternalRow> input) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        FixedBucketRowKeyExtractor extractor = new FixedBucketRowKeyExtractor(schema);

        int numChannels = random.nextInt(10) + 1;
        boolean hasLogSink = random.nextBoolean();
        RowDataChannelComputer channelComputer = new RowDataChannelComputer(schema, hasLogSink);
        channelComputer.setup(numChannels);

        // assert that channel(record) and channel(partition, bucket) gives the same result

        for (InternalRow rowData : input) {
            extractor.setRecord(rowData);
            BinaryRow partition = extractor.partition();
            int bucket = extractor.bucket();

            assertThat(channelComputer.channel(rowData))
                    .isEqualTo(channelComputer.channel(partition, bucket));
        }

        // assert that distribution should be even

        int numTests = random.nextInt(10) + 1;
        for (int test = 0; test < numTests; test++) {
            Map<Integer, Integer> bucketsPerChannel = new HashMap<>();
            for (int i = 0; i < numChannels; i++) {
                bucketsPerChannel.put(i, 0);
            }

            extractor.setRecord(input.get(random.nextInt(input.size())));
            BinaryRow partition = extractor.partition();

            int numBuckets = random.nextInt(numChannels * 4) + 1;
            for (int i = 0; i < numBuckets; i++) {
                int channel = channelComputer.channel(partition, i);
                bucketsPerChannel.compute(channel, (k, v) -> v + 1);
            }

            int max = bucketsPerChannel.values().stream().max(Integer::compareTo).get();
            int min = bucketsPerChannel.values().stream().min(Integer::compareTo).get();
            assertThat(max - min).isLessThanOrEqualTo(1);
        }

        // log sinks like Kafka only consider bucket and don't care about partition
        // so same bucket, even from different partition, must go to the same channel

        if (hasLogSink) {
            Map<Integer, Set<Integer>> channelsPerBucket = new HashMap<>();
            for (InternalRow rowData : input) {
                extractor.setRecord(rowData);
                int bucket = extractor.bucket();
                channelsPerBucket
                        .computeIfAbsent(bucket, k -> new HashSet<>())
                        .add(channelComputer.channel(rowData));
            }
            for (Set<Integer> channels : channelsPerBucket.values()) {
                assertThat(channels).hasSize(1);
            }
        }
    }
}
