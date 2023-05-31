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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.BucketProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link AppendOnlyFileStoreTable} compact worker. */
public class AppendOnlyTableCompactionWorker {
    private final List<AppendOnlyCompactionTask> tasks = new ArrayList<>();
    private final List<CommitMessage> result = new ArrayList<>();
    private final AppendOnlyFileStoreWrite write;
    private final Map<BinaryRow, AppendOnlyCompactManager.CompactRewriter> rewriters =
            new HashMap<>();

    public AppendOnlyTableCompactionWorker(AppendOnlyFileStoreTable table, String commitUser) {
        this.write = table.store().newWrite(commitUser);
    }

    public AppendOnlyTableCompactionWorker accept(List<AppendOnlyCompactionTask> tasks) {
        this.tasks.addAll(tasks);
        return this;
    }

    public AppendOnlyTableCompactionWorker accept(AppendOnlyCompactionTask task) {
        this.tasks.add(task);
        return this;
    }

    public List<CommitMessage> doCompact() throws Exception {
        for (AppendOnlyCompactionTask task : tasks) {
            AppendOnlyCompactManager.CompactRewriter rewriter =
                    rewriters.computeIfAbsent(task.partition(), this::buildRewriter);
            result.add(task.doCompact(rewriter));
        }
        ArrayList<CommitMessage> resultReturn = new ArrayList<>(result);
        result.clear();
        tasks.clear();
        return resultReturn;
    }

    private AppendOnlyCompactManager.CompactRewriter buildRewriter(BinaryRow partition) {
        return write.singleCompactRewriter(partition, BucketProcessor.NON_BUCKET_BUCKET);
    }
}
