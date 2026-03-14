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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.table.sink.CommitMessage;

import javax.annotation.Nullable;

/** {@link CommitMessage} implementation for format table. */
public class TwoPhaseCommitMessage implements CommitMessage {

    private final TwoPhaseOutputStream.Committer committer;

    public TwoPhaseCommitMessage(TwoPhaseOutputStream.Committer committer) {
        this.committer = committer;
    }

    @Override
    public BinaryRow partition() {
        return null;
    }

    @Override
    public int bucket() {
        return 0;
    }

    @Override
    public @Nullable Integer totalBuckets() {
        return 0;
    }

    public TwoPhaseOutputStream.Committer getCommitter() {
        return committer;
    }
}
