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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.catalog.CatalogLock;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.manifest.ManifestCommittableSerializer;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.FileStoreTable;
import org.apache.flink.table.store.table.sink.LogSinkFunction;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Callable;

/** Sink of dynamic store. */
public class StoreSink implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";

    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    private final ObjectIdentifier tableIdentifier;

    private final FileStoreTable table;

    private final boolean compactionTask;

    @Nullable private final Map<String, String> compactPartitionSpec;

    @Nullable private final CatalogLock.Factory lockFactory;

    @Nullable private final Map<String, String> overwritePartition;

    @Nullable private final LogSinkFunction logSinkFunction;

    public StoreSink(
            ObjectIdentifier tableIdentifier,
            FileStoreTable table,
            boolean compactionTask,
            @Nullable Map<String, String> compactPartitionSpec,
            @Nullable CatalogLock.Factory lockFactory,
            @Nullable Map<String, String> overwritePartition,
            @Nullable LogSinkFunction logSinkFunction) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.compactionTask = compactionTask;
        this.compactPartitionSpec = compactPartitionSpec;
        this.lockFactory = lockFactory;
        this.overwritePartition = overwritePartition;
        this.logSinkFunction = logSinkFunction;
    }

    private OneInputStreamOperator<RowData, Committable> createWriteOperator() {
        if (compactionTask) {
            return new StoreCompactOperator(table, compactPartitionSpec);
        }
        return new StoreWriteOperator(table, overwritePartition, logSinkFunction);
    }

    private StoreCommitter createCommitter() {
        CatalogLock catalogLock;
        Lock lock;
        if (lockFactory == null) {
            catalogLock = null;
            lock = null;
        } else {
            catalogLock = lockFactory.create();
            lock =
                    new Lock() {
                        @Override
                        public <T> T runWithLock(Callable<T> callable) throws Exception {
                            return catalogLock.runWithLock(
                                    tableIdentifier.getDatabaseName(),
                                    tableIdentifier.getObjectName(),
                                    callable);
                        }
                    };
        }

        return new StoreCommitter(
                table.newCommit().withOverwritePartition(overwritePartition).withLock(lock),
                catalogLock);
    }

    public DataStreamSink<?> sinkTo(DataStream<RowData> input) {
        CommittableTypeInfo typeInfo = new CommittableTypeInfo();
        SingleOutputStreamOperator<Committable> written =
                input.transform(WRITER_NAME, typeInfo, createWriteOperator())
                        .setParallelism(input.getParallelism());

        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME,
                                typeInfo,
                                new CommitterOperator(
                                        this::createCommitter, ManifestCommittableSerializer::new))
                        .setParallelism(1)
                        .setMaxParallelism(1);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }
}
