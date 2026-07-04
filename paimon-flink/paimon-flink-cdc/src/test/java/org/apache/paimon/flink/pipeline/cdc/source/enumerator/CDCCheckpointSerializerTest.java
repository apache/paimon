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

package org.apache.paimon.flink.pipeline.cdc.source.enumerator;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.flink.pipeline.cdc.source.enumerator.CDCCheckpoint.TableProgress;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.source.FileStoreSourceSplitSerializerTest.newFile;
import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CDCCheckpoint.Serializer}. */
public class CDCCheckpointSerializerTest {
    @Test
    public void testVersion2RoundTrip() throws Exception {
        Identifier identifier = Identifier.create("test_database", "test_table");
        Identifier identifier2 = Identifier.create("test_database", "test_table2");
        List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>();
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
        splits.add(new TableAwareFileStoreSourceSplit("1", dataSplit, 0, identifier, null, 1L));

        Map<Identifier, TableProgress> tableProgressMap = new HashMap<>();
        tableProgressMap.put(identifier, new TableProgress(3L, 1L));
        tableProgressMap.put(identifier2, new TableProgress(null, null));

        CDCCheckpoint checkpoint = new CDCCheckpoint(splits, tableProgressMap);

        CDCCheckpoint.Serializer serializer = new CDCCheckpoint.Serializer();
        CDCCheckpoint newCheckpoint =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));

        assertThat(newCheckpoint).isEqualTo(checkpoint);
    }

    @Test
    public void testDeserializeVersion1() throws Exception {
        Identifier identifier = Identifier.create("test_database", "test_table");
        List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>();
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
                new TableAwareFileStoreSourceSplit("1", dataSplit, 0, identifier, null, 1L);
        splits.add(split);

        byte[] version1Bytes = serializeVersion1(splits, identifier, 3L);

        CDCCheckpoint.Serializer serializer = new CDCCheckpoint.Serializer();
        CDCCheckpoint checkpoint = serializer.deserialize(1, version1Bytes);

        TableAwareFileStoreSourceSplit restoredSplit = checkpoint.getSplits().iterator().next();
        assertThat(restoredSplit.splitId()).isEqualTo(split.splitId());
        assertThat(restoredSplit.split()).isEqualTo(split.split());
        assertThat(restoredSplit.recordsToSkip()).isEqualTo(split.recordsToSkip());
        assertThat(restoredSplit.getIdentifier()).isEqualTo(split.getIdentifier());
        assertThat(restoredSplit.getLastSchemaId()).isEqualTo(split.getLastSchemaId());
        assertThat(restoredSplit.getSchemaId()).isEqualTo(split.getSchemaId());
        assertThat(restoredSplit.schemaChangeEventsToSkip()).isEqualTo(0L);
        assertThat(restoredSplit.isLegacySchemaProgress()).isTrue();
        assertThat(checkpoint.getTableProgressMap())
                .containsEntry(identifier, new TableProgress(3L, null));
    }

    private byte[] serializeVersion1(
            List<TableAwareFileStoreSourceSplit> splits, Identifier identifier, long nextSnapshotId)
            throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        view.writeInt(splits.size());
        for (TableAwareFileStoreSourceSplit split : splits) {
            byte[] bytes = serializeSplitVersion1(split);
            view.writeInt(bytes.length);
            view.write(bytes);
        }

        view.writeInt(1);
        view.writeUTF(JsonSerdeUtil.toJson(identifier));
        view.writeLong(nextSnapshotId);
        return out.toByteArray();
    }

    private byte[] serializeSplitVersion1(TableAwareFileStoreSourceSplit split) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(split.splitId());
        InstantiationUtil.serializeObject(view, split.split());
        view.writeLong(split.recordsToSkip());
        view.writeUTF(JsonSerdeUtil.toJson(split.getIdentifier()));
        view.writeLong(split.getLastSchemaId() == null ? -1L : split.getLastSchemaId());
        view.writeLong(split.getSchemaId());
        return out.toByteArray();
    }
}
