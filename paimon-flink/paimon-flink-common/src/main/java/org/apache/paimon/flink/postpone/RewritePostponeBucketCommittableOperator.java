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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedOneInputOperator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** Rewrite committable from postpone bucket table compactor. */
public class RewritePostponeBucketCommittableOperator
        extends BoundedOneInputOperator<Committable, Committable> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private transient PostponeBucketCommittableRewriter rewriter;

    public RewritePostponeBucketCommittableOperator(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public void open() throws Exception {
        rewriter = new PostponeBucketCommittableRewriter(table);
    }

    @Override
    public void processElement(StreamRecord<Committable> element) throws Exception {
        Committable committable = element.getValue();
        rewriter.add((CommitMessageImpl) committable.commitMessage());
    }

    @Override
    public void endInput() throws Exception {
        emitAll(Long.MAX_VALUE);
    }

    protected void emitAll(long checkpointId) {
        rewriter.emitAll(checkpointId).forEach(c -> output.collect(new StreamRecord<>(c)));
    }
}
