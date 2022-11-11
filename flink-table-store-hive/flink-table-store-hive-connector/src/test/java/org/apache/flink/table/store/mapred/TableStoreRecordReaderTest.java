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

package org.apache.flink.table.store.mapred;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.FileStoreTestUtils;
import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.TableCommit;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.DataSplit;
import org.apache.flink.table.store.utils.BinaryRowDataUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableStoreRecordReader}. */
public class TableStoreRecordReaderTest {

    @TempDir java.nio.file.Path tempDir;
    private String commitUser;

    @BeforeEach
    public void beforeEach() {
        commitUser = UUID.randomUUID().toString();
    }

    @Test
    public void testPk() throws Exception {
        Configuration conf = new Configuration();
        conf.setString(CoreOptions.PATH, tempDir.toString());
        conf.setString(CoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Collections.singletonList("a"));

        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);
        write.write(GenericRowData.of(1L, StringData.fromString("Hi")));
        write.write(GenericRowData.of(2L, StringData.fromString("Hello")));
        write.write(GenericRowData.of(3L, StringData.fromString("World")));
        write.write(GenericRowData.of(1L, StringData.fromString("Hi again")));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 2L, StringData.fromString("Hello")));
        commit.commit(0, write.prepareCommit(true, 0));

        TableStoreRecordReader reader = read(table, BinaryRowDataUtil.EMPTY_ROW, 0);
        RowDataContainer container = reader.createValue();
        Set<String> actual = new HashSet<>();
        while (reader.next(null, container)) {
            RowData rowData = container.get();
            String value = rowData.getLong(0) + "|" + rowData.getString(1).toString();
            actual.add(value);
        }

        Set<String> expected = new HashSet<>();
        expected.add("1|Hi again");
        expected.add("3|World");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testValueCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setString(CoreOptions.PATH, tempDir.toString());
        conf.setString(CoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);
        write.write(GenericRowData.of(1, StringData.fromString("Hi")));
        write.write(GenericRowData.of(2, StringData.fromString("Hello")));
        write.write(GenericRowData.of(3, StringData.fromString("World")));
        write.write(GenericRowData.of(1, StringData.fromString("Hi")));
        write.write(GenericRowData.ofKind(RowKind.DELETE, 2, StringData.fromString("Hello")));
        write.write(GenericRowData.of(1, StringData.fromString("Hi")));
        commit.commit(0, write.prepareCommit(true, 0));

        TableStoreRecordReader reader = read(table, BinaryRowDataUtil.EMPTY_ROW, 0);
        RowDataContainer container = reader.createValue();
        Map<String, Integer> actual = new HashMap<>();
        while (reader.next(null, container)) {
            RowData rowData = container.get();
            String key = rowData.getInt(0) + "|" + rowData.getString(1).toString();
            actual.compute(key, (k, v) -> (v == null ? 0 : v) + 1);
        }

        Map<String, Integer> expected = new HashMap<>();
        expected.put("1|Hi", 3);
        expected.put("3|World", 1);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testProjectionPushdown() throws Exception {
        Configuration conf = new Configuration();
        conf.setString(CoreOptions.PATH, tempDir.toString());
        conf.setString(CoreOptions.FILE_FORMAT, "avro");
        FileStoreTable table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        TableWrite write = table.newWrite(commitUser);
        TableCommit commit = table.newCommit(commitUser);
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        write.write(GenericRowData.of(2, 20L, StringData.fromString("Hello")));
        write.write(GenericRowData.of(1, 10L, StringData.fromString("Hi")));
        commit.commit(0, write.prepareCommit(true, 0));

        TableStoreRecordReader reader =
                read(table, BinaryRowDataUtil.EMPTY_ROW, 0, Arrays.asList("c", "a"));
        RowDataContainer container = reader.createValue();
        Map<String, Integer> actual = new HashMap<>();
        while (reader.next(null, container)) {
            RowData rowData = container.get();
            String key = rowData.getInt(0) + "|" + rowData.getString(2).toString();
            actual.compute(key, (k, v) -> (v == null ? 0 : v) + 1);
        }

        Map<String, Integer> expected = new HashMap<>();
        expected.put("1|Hi", 2);
        expected.put("2|Hello", 1);
        assertThat(actual).isEqualTo(expected);
    }

    private TableStoreRecordReader read(FileStoreTable table, BinaryRowData partition, int bucket)
            throws Exception {
        return read(table, partition, bucket, table.schema().fieldNames());
    }

    private TableStoreRecordReader read(
            FileStoreTable table, BinaryRowData partition, int bucket, List<String> selectedColumns)
            throws Exception {
        for (DataSplit split : table.newScan().plan().splits) {
            if (split.partition().equals(partition) && split.bucket() == bucket) {
                return new TableStoreRecordReader(
                        table.newRead(),
                        new TableStoreInputSplit(tempDir.toString(), split),
                        table.schema().fieldNames(),
                        selectedColumns);
            }
        }
        throw new IllegalArgumentException(
                "Input split not found for partition " + partition + " and bucket " + bucket);
    }
}
