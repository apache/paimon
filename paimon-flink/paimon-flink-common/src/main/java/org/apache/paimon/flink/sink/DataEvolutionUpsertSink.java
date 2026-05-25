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
import org.apache.paimon.flink.dataevolution.UpsertClassifyOperator;
import org.apache.paimon.flink.dataevolution.UpsertRecord;
import org.apache.paimon.flink.dataevolution.UpsertRecordChannelComputer;
import org.apache.paimon.flink.dataevolution.UpsertRecordTypeInfo;
import org.apache.paimon.flink.dataevolution.UpsertWriteOperator;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.paimon.flink.utils.ParallelismUtils.forwardParallelism;

/**
 * A {@link FlinkWriteSink} for data evolution streaming upsert. Uses a two-phase pipeline:
 *
 * <ol>
 *   <li>Phase 1 ({@link UpsertClassifyOperator}): classifies records as INSERT or UPDATE using a
 *       business-key index
 *   <li>Network shuffle by firstRowId to ensure single-writer-per-file
 *   <li>Phase 2 ({@link UpsertWriteOperator}): performs partial writes for updates and appends for
 *       inserts
 * </ol>
 */
public class DataEvolutionUpsertSink extends FlinkWriteSink<InternalRow> {

    private static final long serialVersionUID = 1L;

    private final List<String> upsertKeyColumns;

    public DataEvolutionUpsertSink(
            FileStoreTable table,
            @Nullable Map<String, String> overwritePartition,
            List<String> upsertKeyColumns) {
        super(table, overwritePartition);
        this.upsertKeyColumns = upsertKeyColumns;
    }

    @Override
    public DataStreamSink<?> sinkFrom(DataStream<InternalRow> input, String initialCommitUser) {
        // Phase 1: classify each record as INSERT or UPDATE
        SingleOutputStreamOperator<UpsertRecord> classified =
                input.transform(
                        "Upsert Classify : " + table.name(),
                        new UpsertRecordTypeInfo(table.rowType(), table.partitionKeys().size()),
                        new UpsertClassifyOperator.Factory(table, upsertKeyColumns));
        forwardParallelism(classified, input);

        // Shuffle by firstRowId to guarantee single-writer-per-file
        DataStream<UpsertRecord> shuffled =
                FlinkStreamPartitioner.partition(
                        classified, new UpsertRecordChannelComputer(), null);

        // Phase 2: write (partial updates + inserts)
        SingleOutputStreamOperator<Committable> written =
                shuffled.transform(
                        "Upsert Write : " + table.name(),
                        new CommittableTypeInfo(),
                        new UpsertWriteOperator.Factory(table));
        forwardParallelism(written, shuffled);

        return doCommit(written, initialCommitUser);
    }

    @Override
    protected OneInputStreamOperatorFactory<InternalRow, Committable> createWriteOperatorFactory(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        throw new UnsupportedOperationException(
                "DataEvolutionUpsertSink overrides sinkFrom directly");
    }

    @Override
    protected CommittableStateManager<ManifestCommittable> createCommittableStateManager() {
        return createRestoreOnlyCommittableStateManager(table);
    }
}
