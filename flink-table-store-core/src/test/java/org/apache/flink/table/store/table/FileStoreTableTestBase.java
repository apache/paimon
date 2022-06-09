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

package org.apache.flink.table.store.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.store.file.data.DataFileMeta;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordReaderIterator;
import org.apache.flink.table.store.table.sink.TableWrite;
import org.apache.flink.table.store.table.source.Split;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link FileStoreTable}. */
public abstract class FileStoreTableTestBase {

    protected static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.BIGINT().getLogicalType()
                    },
                    new String[] {"pt", "a", "b"});
    protected static final int[] PROJECTION = new int[] {2, 1};
    protected static final Function<RowData, String> BATCH_ROW_TO_STRING =
            rowData -> rowData.getInt(0) + "|" + rowData.getInt(1) + "|" + rowData.getLong(2);
    protected static final Function<RowData, String> BATCH_PROJECTED_ROW_TO_STRING =
            rowData -> rowData.getLong(0) + "|" + rowData.getInt(1);
    protected static final Function<RowData, String> STREAMING_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_ROW_TO_STRING.apply(rowData);
    protected static final Function<RowData, String> STREAMING_PROJECTED_ROW_TO_STRING =
            rowData ->
                    (rowData.getRowKind() == RowKind.INSERT ? "+" : "-")
                            + BATCH_PROJECTED_ROW_TO_STRING.apply(rowData);

    @Test
    public void testOverwrite() throws Exception {
        FileStoreTable table = createFileStoreTable();

        TableWrite write = table.newWrite(false);
        write.write(GenericRowData.of(1, 10, 100L));
        write.write(GenericRowData.of(2, 20, 200L));
        table.newCommit(null).commit("0", write.prepareCommit());
        write.close();

        write = table.newWrite(true);
        write.write(GenericRowData.of(2, 21, 201L));
        Map<String, String> overwritePartition = new HashMap<>();
        overwritePartition.put("pt", "2");
        table.newCommit(overwritePartition).commit("1", write.prepareCommit());
        write.close();

        List<Split> splits = table.newScan(false).plan().splits;
        TableRead read = table.newRead(false);
        assertThat(getResult(read, splits, binaryRow(1), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("1|10|100"));
        assertThat(getResult(read, splits, binaryRow(2), 0, BATCH_ROW_TO_STRING))
                .hasSameElementsAs(Collections.singletonList("2|21|201"));
    }

    protected List<String> getResult(
            TableRead read,
            List<Split> splits,
            BinaryRowData partition,
            int bucket,
            Function<RowData, String> rowDataToString)
            throws Exception {
        RecordReader<RowData> recordReader =
                read.createReader(partition, bucket, getFilesFor(splits, partition, bucket));
        RecordReaderIterator<RowData> iterator = new RecordReaderIterator<>(recordReader);
        List<String> result = new ArrayList<>();
        while (iterator.hasNext()) {
            RowData rowData = iterator.next();
            result.add(rowDataToString.apply(rowData));
        }
        iterator.close();
        return result;
    }

    private List<DataFileMeta> getFilesFor(
            List<Split> splits, BinaryRowData partition, int bucket) {
        List<DataFileMeta> result = new ArrayList<>();
        for (Split split : splits) {
            if (split.partition().equals(partition) && split.bucket() == bucket) {
                result.addAll(split.files());
            }
        }
        return result;
    }

    protected BinaryRowData binaryRow(int a) {
        BinaryRowData b = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(b);
        writer.writeInt(0, a);
        writer.complete();
        return b;
    }

    protected abstract FileStoreTable createFileStoreTable() throws Exception;
}
