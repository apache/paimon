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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.source.DataSplit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link TableAwareFileStoreSourceSplit} and {@link
 * TableAwareFileStoreSourceSplit.Serializer}.
 */
public class TableAwareFileStoreSourceSplitSerializerTest {
    @Test
    public void test() throws Exception {
        Identifier identifier = Identifier.create("test_database", "test_table");
        DataSplit dataSplit =
                DataSplit.builder()
                        .withSnapshot(1)
                        .withPartition(row(1))
                        .withBucket(2)
                        .withDataFiles(Arrays.asList(newFile(0), newFile(1)))
                        .isStreaming(false)
                        .rawConvertible(false)
                        .withBucketPath("/temp/2") // not used
                        .build();
        TableAwareFileStoreSourceSplit split =
                new TableAwareFileStoreSourceSplit("split-1", dataSplit, 0L, identifier, null, 1L);

        TableAwareFileStoreSourceSplit.Serializer serializer =
                new TableAwareFileStoreSourceSplit.Serializer();
        byte[] serialized = serializer.serialize(split);
        TableAwareFileStoreSourceSplit deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized).isEqualTo(split);
    }
}
