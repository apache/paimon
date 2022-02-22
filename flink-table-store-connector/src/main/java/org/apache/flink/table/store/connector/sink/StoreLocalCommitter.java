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

import org.apache.flink.api.connector.sink2.Committer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.store.connector.sink.global.LocalCommitterOperator.convertCommitRequest;

/** Store local {@link Committer} to commit log sink. */
public class StoreLocalCommitter<LogCommT> implements Committer<Committable> {

    @Nullable private final Committer<LogCommT> logCommitter;

    public StoreLocalCommitter(@Nullable Committer<LogCommT> logCommitter) {
        this.logCommitter = logCommitter;
    }

    @Override
    public void commit(Collection<CommitRequest<Committable>> requests)
            throws IOException, InterruptedException {
        List<CommitRequest<LogCommT>> logRequests = new ArrayList<>();
        for (CommitRequest<Committable> request : requests) {
            if (request.getCommittable().kind() == Committable.Kind.LOG) {
                //noinspection unchecked
                logRequests.add(
                        convertCommitRequest(
                                request,
                                committable -> (LogCommT) committable.wrappedCommittable(),
                                committable -> new Committable(Committable.Kind.LOG, committable)));
            }
        }

        if (logRequests.size() > 0) {
            Objects.requireNonNull(logCommitter, "logCommitter should not be null.");
            logCommitter.commit(logRequests);
        }
    }

    @Override
    public void close() throws Exception {
        if (logCommitter != null) {
            logCommitter.close();
        }
    }
}
