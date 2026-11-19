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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionRangeSortTask;
import org.apache.paimon.flink.action.ActionPartitionPredicate;
import org.apache.paimon.flink.sink.DataEvolutionCompactionTaskTypeInfo;
import org.apache.paimon.flink.sink.DataEvolutionRowIdRewriteSink;
import org.apache.paimon.flink.sink.FlinkSink;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Range;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Sorts complete Data Evolution file groups within an inclusive range of input row IDs. */
public class SortDataEvolutionProcedure extends ProcedureBase {

    @Override
    public String identifier() {
        return "sort_data_evolution";
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "start_row_id", type = @DataTypeHint("BIGINT")),
                @ArgumentHint(name = "end_row_id", type = @DataTypeHint("BIGINT")),
                @ArgumentHint(name = "order_by", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "where", type = @DataTypeHint("STRING"), isOptional = true)
            })
    public String[] call(
            ProcedureContext context,
            String tableId,
            Long startRowId,
            Long endRowId,
            String orderBy,
            String where)
            throws Exception {
        StreamExecutionEnvironment env = context.getExecutionEnvironment();
        checkArgument(!FlinkSink.isStreaming(env), "Sorting data evolution requires batch mode.");
        checkArgument(
                startRowId != null && endRowId != null && orderBy != null,
                "Both row ID bounds and order_by are required.");
        Table target = table(tableId);
        checkArgument(target instanceof FileStoreTable, "Expected a file store table.");
        FileStoreTable table = (FileStoreTable) target;
        List<String> fields = DataEvolutionRangeSortTask.parseSortFields(orderBy);
        DataEvolutionRangeSortTask.validate(table, startRowId, endRowId, fields);
        PartitionPredicate filter =
                ActionPartitionPredicate.create(table, null, nullable(where), identifier());

        Snapshot snapshot = table.snapshotManager().latestSnapshot();
        DataStream<Long> trigger = env.fromSequence(0, 0).setParallelism(1);
        if (snapshot == null) {
            trigger.sinkTo(new DiscardingSink<>());
        } else {
            Range range = new Range(startRowId, endRowId);
            DataStream<DataEvolutionCompactTask> tasks =
                    trigger.flatMap(
                                    (ignored, out) ->
                                            DataEvolutionRangeSortTask.plan(
                                                            table, snapshot, range, filter, fields)
                                                    .forEach(out::collect),
                                    new DataEvolutionCompactionTaskTypeInfo())
                            .name("Plan Data Evolution Sort : " + tableId)
                            .setParallelism(1);
            DataEvolutionRowIdRewriteSink.sink(table, tasks, snapshot);
        }
        return execute(env, "Sort Data Evolution : " + tableId);
    }
}
