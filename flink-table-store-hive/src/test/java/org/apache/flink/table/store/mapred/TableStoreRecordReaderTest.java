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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowDataUtil;
import org.apache.flink.table.store.FileStoreTestHelper;
import org.apache.flink.table.store.RowDataContainer;
import org.apache.flink.table.store.file.FileStoreOptions;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.mergetree.compact.ValueCountMergeFunction;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableStoreRecordReader}. */
public class TableStoreRecordReaderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testPk() throws Exception {
        Configuration conf = new Configuration();
        conf.setString(FileStoreOptions.PATH, tempDir.toString());
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTestHelper helper =
                new FileStoreTestHelper(
                        conf,
                        RowType.of(),
                        RowType.of(
                                new LogicalType[] {DataTypes.BIGINT().getLogicalType()},
                                new String[] {"_KEY_a"}),
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.BIGINT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"a", "b"}),
                        new DeduplicateMergeFunction(),
                        (k, v) -> BinaryRowDataUtil.EMPTY_ROW,
                        k -> 0);

        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1L),
                GenericRowData.of(1L, StringData.fromString("Hi")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2L),
                GenericRowData.of(2L, StringData.fromString("Hello")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(3L),
                GenericRowData.of(3L, StringData.fromString("World")));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1L),
                GenericRowData.of(1L, StringData.fromString("Hi again")));
        helper.write(
                ValueKind.DELETE,
                GenericRowData.of(2L),
                GenericRowData.of(2L, StringData.fromString("Hello")));
        helper.commit();

        Tuple2<RecordReader, Long> tuple = helper.read(BinaryRowDataUtil.EMPTY_ROW, 0);
        TableStoreRecordReader reader = new TableStoreRecordReader(tuple.f0, false, tuple.f1);
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
        conf.setString(FileStoreOptions.PATH, tempDir.toString());
        conf.setString(FileStoreOptions.FILE_FORMAT, "avro");
        FileStoreTestHelper helper =
                new FileStoreTestHelper(
                        conf,
                        RowType.of(),
                        RowType.of(
                                new LogicalType[] {
                                    DataTypes.INT().getLogicalType(),
                                    DataTypes.STRING().getLogicalType()
                                },
                                new String[] {"_KEY_a", "_KEY_b"}),
                        RowType.of(
                                new LogicalType[] {DataTypes.BIGINT().getLogicalType()},
                                new String[] {"_VALUE_COUNT"}),
                        new ValueCountMergeFunction(),
                        (k, v) -> BinaryRowDataUtil.EMPTY_ROW,
                        k -> 0);

        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, StringData.fromString("Hi")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(2, StringData.fromString("Hello")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(3, StringData.fromString("World")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, StringData.fromString("Hi")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.DELETE,
                GenericRowData.of(2, StringData.fromString("Hello")),
                GenericRowData.of(1L));
        helper.write(
                ValueKind.ADD,
                GenericRowData.of(1, StringData.fromString("Hi")),
                GenericRowData.of(1L));
        helper.commit();

        Tuple2<RecordReader, Long> tuple = helper.read(BinaryRowDataUtil.EMPTY_ROW, 0);
        TableStoreRecordReader reader = new TableStoreRecordReader(tuple.f0, true, tuple.f1);
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
}
