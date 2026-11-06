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

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * A wrapper for {@link MergeFunction}, which adds new functionalities or optimizations.
 *
 * @param <T> result type
 */
public interface MergeFunctionWrapper<T> {

    void reset();

    void add(KeyValue kv);

    /** Replays persisted ordering metadata; it must never become an input changelog event. */
    default void addDeleteOrderingGuard(KeyValue guard) {
        throw new UnsupportedOperationException(
                "This merge wrapper cannot resolve delete ordering guards.");
    }

    /** Resolves a native before-image against the same persisted bounds as the current key. */
    default void withDeleteOrderingGuardBeforeImage(Function<KeyValue, KeyValue> resolver) {}

    @Nullable
    T getResult();
}
