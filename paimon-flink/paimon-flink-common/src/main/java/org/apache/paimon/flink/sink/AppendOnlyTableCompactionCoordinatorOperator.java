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

import org.apache.paimon.append.AppendOnlyTableCheckUtils;
import org.apache.paimon.append.AppendOnlyTableCompactionCoordinator;
import org.apache.paimon.append.CompactionTask;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A {@link PrepareCommitOperator} to generate {@link CompactionTask}s. */
public class AppendOnlyTableCompactionCoordinatorOperator
        extends PrepareCommitOperator<Committable> {

    private final FileStoreTable table;
    private transient AppendOnlyTableCompactionCoordinator compactCoordinator;
    private transient List<Committable> result;

    public AppendOnlyTableCompactionCoordinatorOperator(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Committable>> output) {
        super.setup(containingTask, config, output);
        compactCoordinator = AppendOnlyTableCheckUtils.getCompactionCoordinator(table);
        result = new ArrayList<>();
    }

    @Override
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        compactCoordinator.updateRestore();
        List<CompactionTask> tasks = compactCoordinator.compactPlan();
        List<Committable> re =
                tasks.stream()
                        .map(
                                task ->
                                        new Committable(
                                                checkpointId,
                                                Committable.Kind.COMPACTION_TASK,
                                                task))
                        .collect(Collectors.toList());
        re.addAll(result);
        result.clear();
        return re;
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        if (element.getValue().kind() == Committable.Kind.FILE) {
            compactCoordinator.add((CommitMessage) element.getValue().wrappedCommittable());
        } else {
            result.add(element.getValue());
        }
    }
}
