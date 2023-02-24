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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.connector.CatalogITCaseBase;
import org.apache.flink.table.store.connector.FlinkConnectorOptions;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.FileStoreTableFactory;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.table.store.connector.LogicalTypeConversion.toLogicalType;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests of shuffle data by bucket and partition. */
public class FileStoreShuffleBucketTest extends CatalogITCaseBase {
    private static final int TOTAL_SOURCE_RECORD_COUNT = 1000;

    @BeforeEach
    public void after() throws Exception {
        super.before();
        CollectStoreSinkWrite.writeRowsMap.clear();
    }

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE T (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b) NOT ENFORCED) PARTITIONED BY (a)");
    }

    @Test
    public void testShuffleByBucket() throws Exception {
        FileStoreTable table =
                FileStoreTableFactory.create(LocalFileIO.create(), getTableDirectory("T"));

        insertDataToTable(table);

        // Only one task will write records shuffled by bucket
        assertEquals(CollectStoreSinkWrite.writeRowsMap.size(), 1);
    }

    @Test
    public void testShuffleByBucketPartition() throws Exception {
        FileStoreTable originalTable =
                FileStoreTableFactory.create(LocalFileIO.create(), getTableDirectory("T"));
        Map<String, String> dynamicOptions = originalTable.options().toMap();
        dynamicOptions.put(FlinkConnectorOptions.SINK_SHUFFLE_BY_PARTITION.key(), "true");
        FileStoreTable table = originalTable.copy(dynamicOptions);

        insertDataToTable(table);

        // Two tasks will write records shuffled by bucket and partition
        assertEquals(CollectStoreSinkWrite.writeRowsMap.size(), 2);
    }

    private void insertDataToTable(FileStoreTable table) throws Exception {
        RowType rowType = toLogicalType(table.rowType());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(2);

        List<RowData> sourceDataList = generateData();
        DataStreamSource<RowData> sourceStream =
                env.fromCollection(sourceDataList, InternalTypeInfo.of(rowType));
        new FlinkSinkBuilder(table)
                .withInput(sourceStream)
                .withParallelism(env.getParallelism())
                .withSinkProvider(
                        "testUser",
                        (StoreSinkWrite.Provider)
                                (table1, context, ioManager) ->
                                        (StoreSinkWrite) new CollectStoreSinkWrite())
                .build();
        env.execute();

        assertEquals(
                CollectStoreSinkWrite.writeRowsMap.values().stream().mapToInt(List::size).sum(),
                TOTAL_SOURCE_RECORD_COUNT);
    }

    private List<RowData> generateData() {
        List<RowData> rowDataList = new ArrayList<>(TOTAL_SOURCE_RECORD_COUNT);
        for (int i = 0; i < TOTAL_SOURCE_RECORD_COUNT; i++) {
            rowDataList.add(GenericRowData.of(i, i + 1, i + 2, i + 3));
        }
        return rowDataList;
    }

    /** Collect all received data with writer. */
    private static class CollectStoreSinkWrite implements StoreSinkWrite {
        private static final Map<StoreSinkWrite, List<InternalRow>> writeRowsMap =
                new ConcurrentHashMap<>();

        @Override
        public SinkRecord write(InternalRow rowData) throws Exception {
            List<InternalRow> rows = writeRowsMap.computeIfAbsent(this, key -> new ArrayList<>());
            rows.add(rowData);
            return null;
        }

        @Override
        public SinkRecord toLogRecord(SinkRecord record) {
            return record;
        }

        @Override
        public void compact(BinaryRow partition, int bucket, boolean fullCompaction)
                throws Exception {}

        @Override
        public void notifyNewFiles(
                long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files) {}

        @Override
        public List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
                throws IOException {
            return Collections.emptyList();
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {}

        @Override
        public void close() throws Exception {}
    }
}
