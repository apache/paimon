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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.CoreOptions.createCommitUser;

/** {@link FlinkSink} for writing records into postpone bucket table. */
public class PostponeBucketWriteSink extends FlinkWriteSink<InternalRow> {

    public PostponeBucketWriteSink(
            FileStoreTable table, @Nullable Map<String, String> overwritePartition) {
        super(table, overwritePartition);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new PostponeBucketTableWriteOperator.Factory(table, writeProvider, commitUser);
    }

    public DataStreamSink<?> sinkFrom(
            DataStream<InternalRow> input, @Nullable Integer parallelism) {
        String initialCommitUser = createCommitUser(table.coreOptions().toConfiguration());
        DataStream<Committable> written =
                doWrite(
                        input,
                        initialCommitUser,
                        parallelism == null ? input.getParallelism() : parallelism);
        return doCommit(written, initialCommitUser);
    }
}
