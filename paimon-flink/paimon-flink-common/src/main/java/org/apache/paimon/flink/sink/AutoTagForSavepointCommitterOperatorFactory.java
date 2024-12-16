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

import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.utils.SerializableSupplier;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.time.Duration;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * AutoTagForSavepointCommitterOperator}.
 */
public class AutoTagForSavepointCommitterOperatorFactory<CommitT, GlobalCommitT>
        extends AbstractStreamOperatorFactory<CommitT>
        implements OneInputStreamOperatorFactory<CommitT, CommitT> {

    private final CommitterOperatorFactory<CommitT, GlobalCommitT> commitOperatorFactory;

    private final SerializableSupplier<SnapshotManager> snapshotManagerFactory;

    private final SerializableSupplier<TagManager> tagManagerFactory;

    private final SerializableSupplier<TagDeletion> tagDeletionFactory;

    private final SerializableSupplier<List<TagCallback>> callbacksSupplier;

    private final NavigableSet<Long> identifiersForTags;

    private final Duration tagTimeRetained;

    public AutoTagForSavepointCommitterOperatorFactory(
            CommitterOperatorFactory<CommitT, GlobalCommitT> commitOperatorFactory,
            SerializableSupplier<SnapshotManager> snapshotManagerFactory,
            SerializableSupplier<TagManager> tagManagerFactory,
            SerializableSupplier<TagDeletion> tagDeletionFactory,
            SerializableSupplier<List<TagCallback>> callbacksSupplier,
            Duration tagTimeRetained) {
        this.commitOperatorFactory = commitOperatorFactory;
        this.tagManagerFactory = tagManagerFactory;
        this.snapshotManagerFactory = snapshotManagerFactory;
        this.tagDeletionFactory = tagDeletionFactory;
        this.callbacksSupplier = callbacksSupplier;
        this.identifiersForTags = new TreeSet<>();
        this.tagTimeRetained = tagTimeRetained;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<CommitT>> T createStreamOperator(
            StreamOperatorParameters<CommitT> parameters) {
        return (T)
                new AutoTagForSavepointCommitterOperator<>(
                        commitOperatorFactory.createStreamOperator(parameters),
                        snapshotManagerFactory,
                        tagManagerFactory,
                        tagDeletionFactory,
                        callbacksSupplier,
                        tagTimeRetained);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AutoTagForSavepointCommitterOperator.class;
    }
}
