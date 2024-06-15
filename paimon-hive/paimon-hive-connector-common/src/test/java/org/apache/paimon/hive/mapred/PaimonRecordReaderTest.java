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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.FileStoreTestUtils;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonRecordReader}. */
public class PaimonRecordReaderTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testPk() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        conf.set("bucket", "1");
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()},
                                new String[] {"a", "b"}),
                        Collections.emptyList(),
                        Collections.singletonList("a"));

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(GenericRow.of(1L, BinaryString.fromString("Hi")));
        write.write(GenericRow.of(2L, BinaryString.fromString("Hello")));
        write.write(GenericRow.of(3L, BinaryString.fromString("World")));
        write.write(GenericRow.of(1L, BinaryString.fromString("Hi again")));
        write.write(GenericRow.ofKind(RowKind.DELETE, 2L, BinaryString.fromString("Hello")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.close();
        commit.close();

        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0);
        RowDataContainer container = reader.createValue();
        Set<String> actual = new HashSet<>();
        while (reader.next(null, container)) {
            InternalRow rowData = container.get();
            String value = rowData.getLong(0) + "|" + rowData.getString(1).toString();
            actual.add(value);
        }

        Set<String> expected = new HashSet<>();
        expected.add("1|Hi again");
        expected.add("3|World");
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testProjectionPushdown() throws Exception {
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, tempDir.toString());
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()
                                },
                                new String[] {"a", "b", "c"}),
                        Collections.emptyList(),
                        Collections.emptyList());

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        write.write(GenericRow.of(1, 10L, BinaryString.fromString("Hi")));
        write.write(GenericRow.of(2, 20L, BinaryString.fromString("Hello")));
        write.write(GenericRow.of(1, 10L, BinaryString.fromString("Hi")));
        commit.commit(0, write.prepareCommit(true, 0));

        write.close();
        commit.close();

        PaimonRecordReader reader = read(table, BinaryRow.EMPTY_ROW, 0, Arrays.asList("c", "a"));
        RowDataContainer container = reader.createValue();
        Map<String, Integer> actual = new HashMap<>();
        while (reader.next(null, container)) {
            InternalRow rowData = container.get();
            String key = rowData.getInt(0) + "|" + rowData.getString(2).toString();
            actual.compute(key, (k, v) -> (v == null ? 0 : v) + 1);
        }

        Map<String, Integer> expected = new HashMap<>();
        expected.put("1|Hi", 2);
        expected.put("2|Hello", 1);
        assertThat(actual).isEqualTo(expected);
    }

    private PaimonRecordReader read(Table table, BinaryRow partition, int bucket) throws Exception {
        return read(table, partition, bucket, ((FileStoreTable) table).schema().fieldNames());
    }

    private PaimonRecordReader read(
            Table table, BinaryRow partition, int bucket, List<String> selectedColumns)
            throws Exception {
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            DataSplit dataSplit = (DataSplit) split;
            if (dataSplit.partition().equals(partition) && dataSplit.bucket() == bucket) {
                List<String> originalColumns = ((FileStoreTable) table).schema().fieldNames();
                return new PaimonRecordReader(
                        table.newReadBuilder(),
                        new PaimonInputSplit(tempDir.toString(), dataSplit, (FileStoreTable) table),
                        originalColumns,
                        originalColumns,
                        selectedColumns,
                        null);
            }
        }
        throw new IllegalArgumentException(
                "Input split not found for partition " + partition + " and bucket " + bucket);
    }
}
