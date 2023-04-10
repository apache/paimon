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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
                                new HashMap<>(),
                                ""));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        GenericRowData rowData =
                GenericRowData.of(random.nextInt(), random.nextLong(), random.nextDouble());

        testImpl(schema, rowData);
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
                                new HashMap<>(),
                                ""));

        ThreadLocalRandom random = ThreadLocalRandom.current();
        GenericRowData rowData = GenericRowData.of(random.nextLong(), random.nextDouble());

        testImpl(schema, rowData);
    }

    private void testImpl(TableSchema schema, GenericRowData rowData) {
        RowDataKeyAndBucketExtractor extractor = new RowDataKeyAndBucketExtractor(schema);
        extractor.setRecord(rowData);
        BinaryRow partition = extractor.partition();
        int bucket = extractor.bucket();

        int numChannels = ThreadLocalRandom.current().nextInt(10) + 1;

        // assert that channel(record) and channel(partition, bucket) gives the same result

        RowDataChannelComputer channelComputer = new RowDataChannelComputer(schema, true);
        channelComputer.setup(numChannels);
        assertThat(channelComputer.channel(rowData))
                .isEqualTo(channelComputer.channel(partition, bucket));

        channelComputer = new RowDataChannelComputer(schema, false);
        channelComputer.setup(numChannels);
        assertThat(channelComputer.channel(rowData))
                .isEqualTo(channelComputer.channel(partition, bucket));

        // assert that when only shuffle by bucket, distribution should be even

        Set<Integer> usedChannels = new HashSet<>();
        for (int i = 0; i < numChannels; i++) {
            usedChannels.add(channelComputer.channel(partition, i));
        }
        assertThat(usedChannels).hasSize(numChannels);
    }
}
