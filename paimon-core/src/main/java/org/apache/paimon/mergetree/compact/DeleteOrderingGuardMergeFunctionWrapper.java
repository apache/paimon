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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.index.DeleteOrderingGuard;
import org.apache.paimon.index.DeleteOrderingGuardSerializer;
import org.apache.paimon.utils.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * Captures explicit retract ordering before a compaction merge can discard it.
 *
 * <p>Install on sorted compaction inputs after record TTL filtering. Capturing only a merged DELETE
 * would miss stale deletes when a newer insert makes the result live. The collector receives
 * detached, payload-free records in primary-key order, including multiple retracts for one key.
 * Ignored deletes produce no guards.
 *
 * <p>The caller owns the collector's lifecycle: abort its output if compaction fails, and publish
 * the finished segment references atomically with the data changes. This wrapper does not enable
 * table-level guard persistence or change native data and changelog results.
 */
public class DeleteOrderingGuardMergeFunctionWrapper<T> implements MergeFunctionWrapper<T> {

    private final MergeFunctionWrapper<T> wrapped;
    private final DeleteOrderingGuardSerializer serializer;
    private final ThrowingConsumer<DeleteOrderingGuard, IOException> collector;

    public DeleteOrderingGuardMergeFunctionWrapper(
            MergeFunctionWrapper<T> wrapped,
            DeleteOrderingGuardSerializer serializer,
            ThrowingConsumer<DeleteOrderingGuard, IOException> collector) {
        this.wrapped = wrapped;
        this.serializer = serializer;
        this.collector = collector;
    }

    @Override
    public void reset() {
        wrapped.reset();
    }

    @Override
    public void add(KeyValue record) {
        DeleteOrderingGuard guard = serializer.extract(record);
        if (guard != null) {
            try {
                collector.accept(guard);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to capture delete ordering guard.", e);
            }
        }
        wrapped.add(record);
    }

    @Nullable
    @Override
    public T getResult() {
        return wrapped.getResult();
    }

    @Override
    public void addDeleteOrderingGuard(KeyValue guard) {
        wrapped.addDeleteOrderingGuard(guard);
    }

    @Override
    public void withDeleteOrderingGuardBeforeImage(Function<KeyValue, KeyValue> resolver) {
        wrapped.withDeleteOrderingGuardBeforeImage(resolver);
    }
}
