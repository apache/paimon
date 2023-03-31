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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.SinkRecord;
import org.apache.paimon.table.sink.TableWriteImpl;

import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/** Helper class of {@link AbstractStoreWriteOperator} for different types of paimon sinks. */
public interface StoreSinkWrite {

    SinkRecord write(InternalRow rowData) throws Exception;

    SinkRecord toLogRecord(SinkRecord record);

    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    void notifyNewFiles(long snapshotId, BinaryRow partition, int bucket, List<DataFileMeta> files);

    List<Committable> prepareCommit(boolean waitCompaction, long checkpointId) throws IOException;

    void snapshotState(StateSnapshotContext context) throws Exception;

    void close() throws Exception;

    /**
     * Replace the internal {@link TableWriteImpl} with the one provided by {@code
     * newWriteProvider}. The state of the old {@link TableWriteImpl} will also be transferred to
     * the new {@link TableWriteImpl} by {@link TableWriteImpl#checkpoint()} and {@link
     * TableWriteImpl#restore(List)}.
     *
     * <p>Currently, this method is only used by CDC sinks because they need to deal with schema
     * changes. {@link TableWriteImpl} with the new schema will be provided by {@code
     * newWriteProvider}.
     */
    void replace(Function<String, TableWriteImpl<?>> newWriteProvider) throws Exception;

    /** Provider of {@link StoreSinkWrite}. */
    @FunctionalInterface
    interface Provider extends Serializable {

        StoreSinkWrite provide(
                FileStoreTable table, StateInitializationContext context, IOManager ioManager)
                throws Exception;
    }
}
