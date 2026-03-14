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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.SimpleFileEntry;

import javax.annotation.Nullable;

import java.util.List;

/** Need to retry commit of {@link CommitResult}. */
public abstract class RetryCommitResult implements CommitResult {

    public final Exception exception;

    private RetryCommitResult(Exception exception) {
        this.exception = exception;
    }

    public static RetryCommitResult forCommitFail(
            Snapshot snapshot, List<SimpleFileEntry> baseDataFiles, Exception exception) {
        return new CommitFailRetryResult(snapshot, baseDataFiles, exception);
    }

    public static RetryCommitResult forRollback(Exception exception) {
        return new RollbackRetryResult(exception);
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /** Retry result for commit failing. */
    public static class CommitFailRetryResult extends RetryCommitResult {

        public final @Nullable Snapshot latestSnapshot;
        public final @Nullable List<SimpleFileEntry> baseDataFiles;

        private CommitFailRetryResult(
                @Nullable Snapshot latestSnapshot,
                @Nullable List<SimpleFileEntry> baseDataFiles,
                Exception exception) {
            super(exception);
            this.latestSnapshot = latestSnapshot;
            this.baseDataFiles = baseDataFiles;
        }
    }

    /** Retry result for rollback. */
    public static class RollbackRetryResult extends RetryCommitResult {

        private RollbackRetryResult(Exception exception) {
            super(exception);
        }
    }
}
