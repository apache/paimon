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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordChannelComputer}. */
public class CdcRecordChannelComputerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testSchemaWithPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"pt", "k", "v"});

        Options options = new Options();
        options.set(CoreOptions.BUCKET, random.nextInt(1, 5));

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));

        int numInputs = random.nextInt(1000) + 1;
        List<Map<String, String>> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("pt", String.valueOf(random.nextInt(10) + 1));
            fields.put("k", String.valueOf(random.nextLong()));
            fields.put("v", String.valueOf(random.nextDouble()));
            input.add(fields);
        }

        testImpl(schema, input);
    }

    @Test
    public void testSchemaNoPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"k", "v"});

        Options options = new Options();
        options.set(CoreOptions.BUCKET, random.nextInt(1, 5));

        SchemaManager schemaManager =
                new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()));
        TableSchema schema =
                schemaManager.createTable(
                        new Schema(
                                rowType.getFields(),
                                Collections.emptyList(),
                                Collections.singletonList("k"),
                                options.toMap(),
                                ""));

        int numInputs = random.nextInt(1000) + 1;
        List<Map<String, String>> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("k", String.valueOf(random.nextLong()));
            fields.put("v", String.valueOf(random.nextDouble()));
            input.add(fields);
        }

        testImpl(schema, input);
    }

    private void testImpl(TableSchema schema, List<Map<String, String>> input) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        CdcRecordKeyAndBucketExtractor extractor = new CdcRecordKeyAndBucketExtractor(schema);

        int numChannels = random.nextInt(10) + 1;
        CdcRecordChannelComputer channelComputer = new CdcRecordChannelComputer(schema);
        channelComputer.setup(numChannels);

        // assert that channel(record) and channel(partition, bucket) gives the same result

        for (Map<String, String> fields : input) {
            CdcRecord insertRecord = new CdcRecord(RowKind.INSERT, fields);
            CdcRecord deleteRecord = new CdcRecord(RowKind.DELETE, fields);

            extractor.setRecord(random.nextBoolean() ? insertRecord : deleteRecord);
            BinaryRow partition = extractor.partition();
            int bucket = extractor.bucket();

            assertThat(channelComputer.channel(insertRecord))
                    .isEqualTo(channelComputer.channel(partition, bucket));
            assertThat(channelComputer.channel(deleteRecord))
                    .isEqualTo(channelComputer.channel(partition, bucket));
        }

        // assert that distribution should be even

        int numTests = random.nextInt(10) + 1;
        for (int test = 0; test < numTests; test++) {
            Map<Integer, Integer> bucketsPerChannel = new HashMap<>();
            for (int i = 0; i < numChannels; i++) {
                bucketsPerChannel.put(i, 0);
            }

            Map<String, String> fields = input.get(random.nextInt(input.size()));
            extractor.setRecord(new CdcRecord(RowKind.INSERT, fields));
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
    }
}
