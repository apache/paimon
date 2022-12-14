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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.CloseableIterator;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link CompactorSourceBuilder}. */
public class CompactorSourceITCase extends AbstractTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType()
                    },
                    new String[] {"dt", "hh", "k", "v"});

    private Path tablePath;
    private String commitUser;

    @Before
    public void before() throws IOException {
        tablePath = new Path(TEMPORARY_FOLDER.newFolder().toString());
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testBatchRead() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(20221208, 15, 1, 1510));
        write.write(rowData(20221208, 16, 2, 1620));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(20221208, 15, 1, 1511));
        write.write(rowData(20221209, 15, 1, 1510));
        commit.commit(1, write.prepareCommit(true, 1));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> compactorSource =
                new CompactorSourceBuilder("test", table)
                        .withContinuousMode(false)
                        .withEnv(env)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        while (it.hasNext()) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList("+I 20221208|15|0", "+I 20221208|16|0", "+I 20221209|15|0"));

        write.close();
        commit.close();
        it.close();
    }

    @Test
    public void testStreamingRead() throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(20221208, 15, 1, 1510));
        write.write(rowData(20221208, 16, 2, 1620));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(20221208, 15, 1, 1511));
        write.write(rowData(20221209, 15, 1, 1510));
        write.compact(binaryRow(20221208, 15), 0, true);
        write.compact(binaryRow(20221209, 15), 0, true);
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(20221208, 15, 2, 1520));
        write.write(rowData(20221208, 16, 2, 1621));
        commit.commit(2, write.prepareCommit(true, 2));

        write.write(rowData(20221208, 15, 1, 1512));
        write.write(rowData(20221209, 16, 2, 1620));
        commit.commit(3, write.prepareCommit(true, 3));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> compactorSource =
                new CompactorSourceBuilder("test", table)
                        .withContinuousMode(true)
                        .withEnv(env)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual)
                .hasSameElementsAs(
                        Arrays.asList(
                                "+I 20221208|15|0",
                                "+I 20221208|16|0",
                                "+I 20221208|15|0",
                                "+I 20221209|16|0"));

        write.write(rowData(20221209, 15, 2, 1520));
        write.write(rowData(20221208, 16, 1, 1510));
        write.write(rowData(20221209, 15, 1, 1511));
        commit.commit(4, write.prepareCommit(true, 4));

        actual.clear();
        for (int i = 0; i < 2; i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual).hasSameElementsAs(Arrays.asList("+I 20221208|16|0", "+I 20221209|15|0"));

        write.close();
        commit.close();

        write = table.newWrite(commitUser).withOverwrite(true);
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("dt", "20221209");
        partitionMap.put("hh", "16");
        commit = table.newCommit(commitUser).withOverwritePartition(partitionMap);
        write.write(rowData(20221209, 16, 1, 1512));
        write.write(rowData(20221209, 16, 2, 1622));
        commit.commit(5, write.prepareCommit(true, 5));

        assertThat(toString(it.next())).isEqualTo("+I 20221209|16|0");

        write.close();
        commit.close();
        it.close();
    }

    @Test
    public void testStreamingPartitionSpec() throws Exception {
        testPartitionSpec(
                true,
                getSpecifiedPartitions(),
                Arrays.asList(
                        "+I 20221208|16|0",
                        "+I 20221209|15|0",
                        "+I 20221208|16|0",
                        "+I 20221209|15|0"));
    }

    @Test
    public void testBatchPartitionSpec() throws Exception {
        testPartitionSpec(
                false,
                getSpecifiedPartitions(),
                Arrays.asList("+I 20221208|16|0", "+I 20221209|15|0"));
    }

    private List<Map<String, String>> getSpecifiedPartitions() {
        Map<String, String> partition1 = new HashMap<>();
        partition1.put("dt", "20221208");
        partition1.put("hh", "16");

        Map<String, String> partition2 = new HashMap<>();
        partition2.put("dt", "20221209");
        partition2.put("hh", "15");

        return Arrays.asList(partition1, partition2);
    }

    private void testPartitionSpec(
            boolean isStreaming,
            List<Map<String, String>> specifiedPartitions,
            List<String> expected)
            throws Exception {
        FileStoreTable table = createFileStoreTable();
        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);

        write.write(rowData(20221208, 15, 1, 1510));
        write.write(rowData(20221208, 16, 2, 1620));
        commit.commit(0, write.prepareCommit(true, 0));

        write.write(rowData(20221208, 15, 2, 1520));
        write.write(rowData(20221209, 15, 2, 1520));
        commit.commit(1, write.prepareCommit(true, 1));

        write.write(rowData(20221208, 15, 1, 1511));
        write.write(rowData(20221208, 16, 1, 1610));
        write.write(rowData(20221209, 15, 1, 1510));
        commit.commit(2, write.prepareCommit(true, 2));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<RowData> compactorSource =
                new CompactorSourceBuilder("test", table)
                        .withContinuousMode(isStreaming)
                        .withEnv(env)
                        .withPartitions(specifiedPartitions)
                        .build();
        CloseableIterator<RowData> it = compactorSource.executeAndCollect();

        List<String> actual = new ArrayList<>();
        for (int i = 0; i < expected.size(); i++) {
            actual.add(toString(it.next()));
        }
        assertThat(actual).hasSameElementsAs(expected);

        write.close();
        commit.close();
        it.close();
    }

    private String toString(RowData rowData) {
        return String.format(
                "%s %d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getInt(0),
                rowData.getInt(1),
                rowData.getInt(2));
    }

    private GenericRowData rowData(Object... values) {
        return GenericRowData.of(values);
    }

    private BinaryRowData binaryRow(int dt, int hh) {
        BinaryRowData b = new BinaryRowData(2);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeInt(0, dt);
        writer.writeInt(1, hh);
        writer.complete();
        return b;
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        SchemaManager schemaManager = new SchemaManager(tablePath);
        TableSchema tableSchema =
                schemaManager.commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Arrays.asList("dt", "hh"),
                                Arrays.asList("dt", "hh", "k"),
                                Collections.emptyMap(),
                                ""));
        return FileStoreTableFactory.create(tablePath, tableSchema);
    }
}
