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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.lookup.rocksdb.RocksDBOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Test for {@link DynamicPartitionLevelLoader}. */
public class DynamicPartitionLevelLoaderTest {

    @TempDir private Path tempDir;

    private final String commitUser = UUID.randomUUID().toString();
    private final TraceableFileIO fileIO = new TraceableFileIO();

    private org.apache.paimon.fs.Path tablePath;
    private FileStoreTable table;

    @BeforeEach
    public void before() throws Exception {
        tablePath = new org.apache.paimon.fs.Path(tempDir.toString());
    }

    @Test
    public void testGetMaxPartitions() throws Exception {
        List<String> partitionKeys = Arrays.asList("pt1", "pt2", "pt3");
        List<String> primaryKeys = Arrays.asList("pt1", "pt2", "pt3", "k");
        table = createFileStoreTable(partitionKeys, primaryKeys, Collections.emptyMap());

        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(BinaryString.fromString("2025"), 16, 2, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2024"), 15, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 2, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 16, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2024"), 16, 1, 1, 1L));
        commit.commit(1, write.prepareCommit(true, 1));

        // test specify first-level partition
        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(FlinkConnectorOptions.SCAN_PARTITIONS.key(), "pt1=max_pt()");
        table = table.copy(customOptions);

        DynamicPartitionLevelLoader partitionLoader =
                (DynamicPartitionLevelLoader) PartitionLoader.of(table);
        partitionLoader.open();
        List<BinaryRow> partitions = partitionLoader.getMaxPartitions();
        assertThat(partitions.size()).isEqualTo(4);
        assertThat(partitionsToString(partitions))
                .hasSameElementsAs(
                        Arrays.asList("2025/16/2", "2025/16/1", "2025/15/2", "2025/15/1"));

        // test specify first-level and second-level partition
        customOptions.put(FlinkConnectorOptions.SCAN_PARTITIONS.key(), "pt1=max_pt(),pt2=max_pt()");
        table = table.copy(customOptions);
        partitionLoader = (DynamicPartitionLevelLoader) PartitionLoader.of(table);
        partitionLoader.open();
        partitions = partitionLoader.getMaxPartitions();
        assertThat(partitions.size()).isEqualTo(2);
        assertThat(partitionsToString(partitions))
                .hasSameElementsAs(Arrays.asList("2025/16/2", "2025/16/1"));

        // test specify all level partition
        customOptions.put(
                FlinkConnectorOptions.SCAN_PARTITIONS.key(),
                "pt1=max_pt(),pt2=max_pt(),pt3=max_pt()");
        table = table.copy(customOptions);

        partitionLoader = (DynamicPartitionLevelLoader) PartitionLoader.of(table);
        partitionLoader.open();
        partitions = partitionLoader.getMaxPartitions();
        assertThat(partitions.size()).isEqualTo(1);
        assertThat(partitionsToString(partitions)).hasSameElementsAs(Arrays.asList("2025/16/2"));

        write.close();
        commit.close();
    }

    @Test
    public void testGetMaxPartitionsWhenNullPartition() throws Exception {
        List<String> partitionKeys = Arrays.asList("pt1", "pt2", "pt3");
        table =
                createFileStoreTable(
                        partitionKeys, Collections.emptyList(), Collections.emptyMap());

        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 2, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, null, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), null, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2024"), 15, 1, 1, 1L));
        write.write(GenericRow.of(null, 16, 1, 1, 1L));
        commit.commit(1, write.prepareCommit(true, 1));

        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(FlinkConnectorOptions.SCAN_PARTITIONS.key(), "pt1=max_pt(),pt2=max_pt()");
        table = table.copy(customOptions);

        DynamicPartitionLevelLoader partitionLoader =
                (DynamicPartitionLevelLoader) PartitionLoader.of(table);
        partitionLoader.open();
        List<BinaryRow> partitions = partitionLoader.getMaxPartitions();
        assertThat(partitions.size()).isEqualTo(3);
        assertThat(partitionsToString(partitions))
                .hasSameElementsAs(Arrays.asList("2025/15/2", "2025/15/1", "2025/15/null"));

        write.write(GenericRow.of(BinaryString.fromString("2026"), null, null, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2026"), null, 1, 1, 1L));
        commit.commit(2, write.prepareCommit(true, 2));
        partitionLoader = (DynamicPartitionLevelLoader) PartitionLoader.of(table);
        partitionLoader.open();
        partitions = partitionLoader.getMaxPartitions();
        assertThat(partitions.size()).isEqualTo(2);
        assertThat(partitionsToString(partitions))
                .hasSameElementsAs(Arrays.asList("2026/null/1", "2026/null/null"));

        write.close();
        commit.close();
    }

    @Test
    public void testWrongConfig() throws Exception {
        List<String> partitionKeys = Arrays.asList("pt1", "pt2", "pt3");
        table =
                createFileStoreTable(
                        partitionKeys, Collections.emptyList(), Collections.emptyMap());

        TableWriteImpl<?> write = table.newWrite(commitUser);
        TableCommitImpl commit = table.newCommit(commitUser);
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, 2, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), 15, null, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2025"), null, 1, 1, 1L));
        write.write(GenericRow.of(BinaryString.fromString("2024"), 15, 1, 1, 1L));
        write.write(GenericRow.of(null, 16, 1, 1, 1L));
        commit.commit(1, write.prepareCommit(true, 1));

        Map<String, String> customOptions = new HashMap<>();
        customOptions.put(FlinkConnectorOptions.SCAN_PARTITIONS.key(), "pt1=max_pt(),pt3=max_pt()");
        table = table.copy(customOptions);

        assertThatCode(() -> PartitionLoader.of(table))
                .hasMessage(
                        "partition field(level=1,name=pt2) don't set config, "
                                + "but the sub partition field(level=2,name=pt3) set config, this is unsupported.");

        write.close();
        commit.close();
    }

    private FileStoreTable createFileStoreTable(
            List<String> partitionKeys, List<String> primaryKeys, Map<String, String> customOptions)
            throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        Options conf = new Options(customOptions);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(RocksDBOptions.LOOKUP_CONTINUOUS_DISCOVERY_INTERVAL, Duration.ofSeconds(1));
        if (primaryKeys.isEmpty()) {
            conf.set(CoreOptions.BUCKET_KEY.key(), "k");
        }

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.INT(),
                            DataTypes.BIGINT()
                        },
                        new String[] {"pt1", "pt2", "pt3", "k", "v"});
        Schema schema =
                new Schema(rowType.getFields(), partitionKeys, primaryKeys, conf.toMap(), "");
        TableSchema tableSchema = schemaManager.createTable(schema);
        return FileStoreTableFactory.create(
                fileIO, new org.apache.paimon.fs.Path(tempDir.toString()), tableSchema);
    }

    private List<String> partitionsToString(List<BinaryRow> partitions) {
        return partitions.stream()
                .map(
                        partition ->
                                InternalRowPartitionComputer.partToSimpleString(
                                        table.rowType().project(table.partitionKeys()),
                                        partition,
                                        "/",
                                        200))
                .collect(Collectors.toList());
    }
}
