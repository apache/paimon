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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.append.SortCompactCommitMessageRewriter;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;

/** Commit orchestration for Spark sort compact writes. */
final class SortCompactSparkCommit {

    private SortCompactSparkCommit() {}

    static void commit(
            SortCompactCommitMessageRewriter rewriter,
            TableCommitter tableCommitter,
            PostCommitter postCommitter,
            List<CommitMessage> writtenMessages) {
        List<CommitMessage> compactMessages;
        try {
            compactMessages = rewriter.rewrite(writtenMessages);
        } catch (Exception e) {
            abortQuietly(rewriter, writtenMessages, e);
            throw new RuntimeException(e);
        }

        long snapshotIdBeforeCommit = rewriter.latestSnapshotIdOrZero();
        try {
            tableCommitter.commitTable(compactMessages);
        } catch (RuntimeException e) {
            // TableCommitImpl commits the snapshot before maintenance/close. Aborting write output
            // after a successful snapshot commit would delete files referenced by the COMPACT
            // snapshot and corrupt the table.
            if (!rewriter.isBatchCompactCommitSucceeded(snapshotIdBeforeCommit, compactMessages)) {
                abortQuietly(rewriter, writtenMessages, e);
            }
            throw e;
        }

        try {
            postCommitter.postCommit(compactMessages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void abortQuietly(
            SortCompactCommitMessageRewriter rewriter,
            List<CommitMessage> writtenMessages,
            Exception e) {
        try {
            rewriter.abortWrittenMessages(writtenMessages);
        } catch (Exception abortException) {
            e.addSuppressed(abortException);
        }
    }

    @FunctionalInterface
    interface TableCommitter {
        void commitTable(List<CommitMessage> compactMessages);
    }

    @FunctionalInterface
    interface PostCommitter {
        void postCommit(List<CommitMessage> compactMessages);
    }
}
