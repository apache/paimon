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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostponeFixedBucketChannelComputer}. */
public class PostponeFixedBucketChannelComputerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testRecordsDistributedAcrossChannels() throws Exception {
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
                                new HashMap<String, String>() {
                                    {
                                        put("bucket", "-1");
                                        put("postpone.bucket-mode", "true");
                                    }
                                },
                                ""));

        int numChannels = 8;
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        PostponeFixedBucketChannelComputer computer =
                new PostponeFixedBucketChannelComputer(schema, knownNumBuckets);
        computer.setup(numChannels);

        Set<Integer> channels = new HashSet<>();
        for (long i = 0; i < 100; i++) {
            InternalRow row = GenericRow.of(1, i, (double) i);
            int channel = computer.channel(row);
            assertThat(channel).isGreaterThanOrEqualTo(0).isLessThan(numChannels);
            channels.add(channel);
        }

        // With 100 distinct keys and 8 channels, we should hit more than 1 channel
        assertThat(channels.size()).isGreaterThan(1);
    }

    @Test
    public void testNoPartitionDistribution() throws Exception {
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
                                new HashMap<String, String>() {
                                    {
                                        put("bucket", "-1");
                                        put("postpone.bucket-mode", "true");
                                    }
                                },
                                ""));

        int numChannels = 8;
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        PostponeFixedBucketChannelComputer computer =
                new PostponeFixedBucketChannelComputer(schema, knownNumBuckets);
        computer.setup(numChannels);

        Set<Integer> channels = new HashSet<>();
        for (long i = 0; i < 100; i++) {
            InternalRow row = GenericRow.of(i, (double) i);
            int channel = computer.channel(row);
            assertThat(channel).isGreaterThanOrEqualTo(0).isLessThan(numChannels);
            channels.add(channel);
        }

        // Without the fix, all records would go to the same channel
        // With the fix, 100 distinct keys across 8 channels should use multiple channels
        assertThat(channels.size()).isGreaterThan(1);
    }

    @Test
    public void testSameKeyGoesToSameChannel() throws Exception {
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
                                new HashMap<String, String>() {
                                    {
                                        put("bucket", "-1");
                                        put("postpone.bucket-mode", "true");
                                    }
                                },
                                ""));

        int numChannels = 8;
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        PostponeFixedBucketChannelComputer computer =
                new PostponeFixedBucketChannelComputer(schema, knownNumBuckets);
        computer.setup(numChannels);

        // Same key should always route to the same channel
        for (long key = 0; key < 50; key++) {
            InternalRow row1 = GenericRow.of(key, 1.0);
            InternalRow row2 = GenericRow.of(key, 2.0);
            assertThat(computer.channel(row1)).isEqualTo(computer.channel(row2));
        }
    }
}
