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

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.SinkRecord;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** Helper class of {@link StoreWriteOperator} for different types of table store sinks. */
interface StoreSinkWrite {

    SinkRecord write(RowData rowData) throws Exception;

    SinkRecord toLogRecord(SinkRecord record);

    void compact(BinaryRowData partition, int bucket, boolean fullCompaction) throws Exception;

    void notifyNewFiles(
            long snapshotId, BinaryRowData partition, int bucket, List<DataFileMeta> files);

    List<Committable> prepareCommit(boolean doCompaction, long checkpointId) throws IOException;

    void snapshotState(StateSnapshotContext context) throws Exception;

    void close() throws Exception;

    @FunctionalInterface
    interface Provider extends Serializable {

        StoreSinkWrite provide(
                FileStoreTable table, StateInitializationContext context, IOManager ioManager)
                throws Exception;
    }
}
