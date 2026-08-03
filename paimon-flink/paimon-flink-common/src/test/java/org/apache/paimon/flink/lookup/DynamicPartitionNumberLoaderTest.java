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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
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
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DynamicPartitionNumberLoader}. */
class DynamicPartitionNumberLoaderTest {

    @TempDir private Path tempDir;

    private final String commitUser = UUID.randomUUID().toString();
    private final TraceableFileIO fileIO = new TraceableFileIO();

    private FileStoreTable table;
    private TableWriteImpl<?> write;
    private TableCommitImpl commit;

    @BeforeEach
    void before() throws Exception {
        table = createFileStoreTable();
        write = table.newWrite(commitUser);
        commit = table.newCommit(commitUser);
    }

    @AfterEach
    void after() throws Exception {
        write.close();
        commit.close();
    }

    @Test
    void testMaxTwoPartitionsAndRefresh() throws Exception {
        writePartition("2024", 1);
        writePartition("2025", 2);
        commit.commit(1, write.prepareCommit(true, 1));

        DynamicPartitionLoader loader = createLoader("max_two_pt()");
        assertThat(loader.checkRefresh()).isTrue();
        assertThat(partitions(loader)).containsExactly("2025", "2024");
        assertThat(loader.checkRefresh()).isFalse();

        writePartition("2026", 3);
        commit.commit(2, write.prepareCommit(true, 2));

        assertThat(loader.checkRefresh()).isTrue();
        assertThat(partitions(loader)).containsExactly("2026", "2025");
    }

    @Test
    void testMaxPartition() throws Exception {
        writePartition("2024", 1);
        writePartition("2025", 2);
        commit.commit(1, write.prepareCommit(true, 1));

        DynamicPartitionLoader loader = createLoader("max_pt()");
        assertThat(loader.checkRefresh()).isTrue();
        assertThat(partitions(loader)).containsExactly("2025");
    }

    private DynamicPartitionLoader createLoader(String scanPartitions) {
        FileStoreTable configuredTable =
                table.copy(
                        Collections.singletonMap(
                                FlinkConnectorOptions.SCAN_PARTITIONS.key(), scanPartitions));
        DynamicPartitionLoader loader =
                (DynamicPartitionLoader) PartitionLoader.of(configuredTable);
        loader.open();
        return loader;
    }

    private void writePartition(String partition, int value) throws Exception {
        write.write(GenericRow.of(BinaryString.fromString(partition), value, (long) value));
    }

    private List<String> partitions(DynamicPartitionLoader loader) {
        return loader.partitions().stream()
                .map(row -> row.getString(0))
                .map(BinaryString::toString)
                .collect(Collectors.toList());
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        org.apache.paimon.fs.Path tablePath = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(FlinkConnectorOptions.LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL, Duration.ZERO);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});
        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        options.toMap(),
                        "");
        TableSchema tableSchema = schemaManager.createTable(schema);
        return FileStoreTableFactory.create(fileIO, tablePath, tableSchema);
    }
}
