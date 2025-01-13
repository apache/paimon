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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.service.network.NetworkUtils;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** Operator for query executor. */
public class QueryExecutorOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow> {

    private static final long serialVersionUID = 1L;

    private final Table table;

    private transient LocalTableQuery query;

    private transient IOManager ioManager;

    public QueryExecutorOperator(Table table) {
        this.table = table;
    }

    public static RowType outputType() {
        return RowType.of(DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        this.ioManager =
                IOManager.create(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        this.query = ((FileStoreTable) table).newLocalTableQuery().withIOManager(ioManager);
        KvQueryServer server =
                new KvQueryServer(
                        RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
                        RuntimeContextUtils.getNumberOfParallelSubtasks(getRuntimeContext()),
                        NetworkUtils.findHostAddress(),
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());

        try {
            server.start();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        InetSocketAddress address = server.getServerAddress();
        this.output.collect(
                new StreamRecord<>(
                        GenericRow.of(
                                RuntimeContextUtils.getNumberOfParallelSubtasks(
                                        getRuntimeContext()),
                                RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()),
                                BinaryString.fromString(address.getHostName()),
                                address.getPort())));
    }

    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow row = streamRecord.getValue();
        BinaryRow partition = deserializeBinaryRow(row.getBinary(1));
        int bucket = row.getInt(2);
        DataFileMetaSerializer fileMetaSerializer = new DataFileMetaSerializer();
        List<DataFileMeta> beforeFiles = fileMetaSerializer.deserializeList(row.getBinary(3));
        List<DataFileMeta> dataFiles = fileMetaSerializer.deserializeList(row.getBinary(4));
        query.refreshFiles(partition, bucket, beforeFiles, dataFiles);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (query != null) {
            query.close();
        }
        if (ioManager != null) {
            ioManager.close();
        }
    }
}
