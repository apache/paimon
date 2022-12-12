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

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.operation.Lock;
import org.apache.flink.table.store.table.FileStoreTable;

/** {@link AbstractFlinkSink} for stand-alone compact jobs. */
public class CompactorSink extends AbstractFlinkSink {

    private static final long serialVersionUID = 1L;

    private final Lock.Factory lockFactory;

    public CompactorSink(FileStoreTable table, Lock.Factory lockFactory) {
        super(table, false);
        this.lockFactory = lockFactory;
    }

    @Override
    protected OneInputStreamOperator<RowData, Committable> createWriteOperator(
            String initialCommitUser, boolean isStreaming) {
        return new StoreCompactOperator(table, createWriteProvider(initialCommitUser), isStreaming);
    }

    @Override
    protected OneInputStreamOperator<Committable, Committable> createCommitterOperator(
            String initialCommitUser, boolean streamingCheckpointEnabled) {
        return new AtMostOnceCommitterOperator(
                streamingCheckpointEnabled, initialCommitUser, this::createCommitter);
    }

    private StoreCommitter createCommitter(String user) {
        return new StoreCommitter(table.newCommit(user).withLock(lockFactory.create()));
    }
}
