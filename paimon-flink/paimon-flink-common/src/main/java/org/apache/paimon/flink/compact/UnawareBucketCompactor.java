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

package org.apache.paimon.flink.compact;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.operation.AppendOnlyFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The Compactor of unaware bucket table to execute {@link UnawareAppendCompactionTask}. */
public class UnawareBucketCompactor {

    private final FileStoreTable table;
    private final String commitUser;

    private final transient AppendOnlyFileStoreWrite write;

    protected final transient Queue<Future<CommitMessage>> result;

    private final transient Supplier<ExecutorService> compactExecutorsupplier;

    public UnawareBucketCompactor(
            FileStoreTable table,
            String commitUser,
            Supplier<ExecutorService> lazyCompactExecutor) {
        this.table = table;
        this.commitUser = commitUser;
        this.write = (AppendOnlyFileStoreWrite) table.store().newWrite(commitUser);
        this.result = new LinkedList<>();
        this.compactExecutorsupplier = lazyCompactExecutor;
    }

    public void processElement(UnawareAppendCompactionTask task) throws Exception {
        result.add(compactExecutorsupplier.get().submit(() -> task.doCompact(table, write)));
    }

    public void close() throws Exception {
        shutdown();
    }

    @VisibleForTesting
    void shutdown() throws Exception {

        List<CommitMessage> messages = new ArrayList<>();
        for (Future<CommitMessage> resultFuture : result) {
            if (!resultFuture.isDone()) {
                // the later tasks should be stopped running
                break;
            }
            try {
                messages.add(resultFuture.get());
            } catch (Exception exception) {
                // exception should already be handled
            }
        }
        if (messages.isEmpty()) {
            return;
        }

        try (TableCommitImpl tableCommit = table.newCommit(commitUser)) {
            tableCommit.abort(messages);
        }
    }

    public List<Committable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<CommitMessage> tempList = new ArrayList<>();
        try {
            while (!result.isEmpty()) {
                Future<CommitMessage> future = result.peek();
                if (!future.isDone() && !waitCompaction) {
                    break;
                }
                result.poll();
                tempList.add(future.get());
            }
            return tempList.stream()
                    .map(s -> new Committable(checkpointId, Committable.Kind.FILE, s))
                    .collect(Collectors.toList());
        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted while waiting tasks done.", e);
        } catch (Exception e) {
            throw new RuntimeException("Encountered an error while do compaction", e);
        }
    }

    public Iterable<Future<CommitMessage>> result() {
        return result;
    }
}
