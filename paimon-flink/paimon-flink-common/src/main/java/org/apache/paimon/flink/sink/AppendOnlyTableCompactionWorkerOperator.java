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

import org.apache.paimon.append.AppendOnlyCompactionTask;
import org.apache.paimon.append.AppendOnlyTableCompactionWorker;
import org.apache.paimon.flink.source.BucketUnawareCompactSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Operator to execute {@link AppendOnlyCompactionTask} passed from {@link
 * BucketUnawareCompactSource}.
 */
public class AppendOnlyTableCompactionWorkerOperator
        extends PrepareCommitOperator<AppendOnlyCompactionTask, Committable> {
    private final AppendOnlyFileStoreTable table;
    private transient AppendOnlyTableCompactionWorker worker;
    private final String commitUser;
    private transient List<CommitMessage> result;

    public AppendOnlyTableCompactionWorkerOperator(
            AppendOnlyFileStoreTable table, String commitUser) {
        super(Options.fromMap(table.options()));
        this.table = table;
        this.commitUser = commitUser;
    }

    @Override
    public void open() throws Exception {
        this.worker = new AppendOnlyTableCompactionWorker(table, commitUser);
        this.result = new ArrayList<>();
    }

    @Override
    protected List<Committable> prepareCommit(boolean doCompaction, long checkpointId)
            throws IOException {
        // ignore doCompaction tag
        ArrayList<CommitMessage> tempList = new ArrayList<>(result);
        result.clear();
        return tempList.stream()
                .map(s -> new Committable(checkpointId, Committable.Kind.FILE, s))
                .collect(Collectors.toList());
    }

    @Override
    public void processElement(StreamRecord<AppendOnlyCompactionTask> element) throws Exception {
        AppendOnlyCompactionTask task = element.getValue();
        worker.accept(task);
        result.addAll(worker.doCompact());
    }
}
