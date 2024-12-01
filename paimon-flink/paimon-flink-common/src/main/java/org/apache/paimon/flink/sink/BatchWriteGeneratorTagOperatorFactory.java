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

import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * BatchWriteGeneratorTagOperator}.
 */
public class BatchWriteGeneratorTagOperatorFactory<CommitT, GlobalCommitT>
        extends AbstractStreamOperatorFactory<CommitT>
        implements OneInputStreamOperatorFactory<CommitT, CommitT> {
    private final CommitterOperatorFactory<CommitT, GlobalCommitT> commitOperatorFactory;

    protected final FileStoreTable table;

    public BatchWriteGeneratorTagOperatorFactory(
            CommitterOperatorFactory<CommitT, GlobalCommitT> commitOperatorFactory,
            FileStoreTable table) {
        this.table = table;
        this.commitOperatorFactory = commitOperatorFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommitT>> T createStreamOperator(
            StreamOperatorParameters<CommitT> parameters) {
        return (T)
                new BatchWriteGeneratorTagOperator<>(
                        commitOperatorFactory.createStreamOperator(parameters), table);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return BatchWriteGeneratorTagOperator.class;
    }
}
