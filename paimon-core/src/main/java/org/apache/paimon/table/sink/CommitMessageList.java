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

package org.apache.paimon.table.sink;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Internal, read-only view of task results. Unless explicitly materialized for extensions, decoded
 * messages are not cached: each traversal can release the preceding message. All serialized bytes
 * and the largest single message still need to fit in memory. Callers must not modify the backing
 * messages or byte arrays during commit.
 */
public final class CommitMessageList extends AbstractList<CommitMessage> implements RandomAccess {

    private final int size;
    private final IntFunction<CommitMessage> getter;
    private final Supplier<Iterator<CommitMessage>> iterators;
    private List<CommitMessage> materialized;

    private CommitMessageList(
            int size,
            IntFunction<CommitMessage> getter,
            Supplier<Iterator<CommitMessage>> iterators) {
        this.size = size;
        this.getter = getter;
        this.iterators = iterators;
    }

    public static CommitMessageList fromBytes(List<byte[]> messages) {
        List<byte[]> bytes = new ArrayList<>(messages);
        return new CommitMessageList(
                bytes.size(),
                index -> deserialize(new CommitMessageSerializer(), bytes.get(index)),
                () -> {
                    CommitMessageSerializer serializer = new CommitMessageSerializer();
                    return Iterators.transform(bytes.iterator(), b -> deserialize(serializer, b));
                });
    }

    public static CommitMessageList concat(List<List<CommitMessage>> messages) {
        List<List<CommitMessage>> children = new ArrayList<>(messages);
        int[] ends = new int[children.size()];
        int size = 0;
        for (int i = 0; i < children.size(); i++) {
            size = Math.addExact(size, children.get(i).size());
            ends[i] = size;
        }
        return new CommitMessageList(
                size,
                index -> {
                    int low = 0;
                    int high = ends.length;
                    while (low < high) {
                        int mid = (low + high) >>> 1;
                        if (ends[mid] <= index) {
                            low = mid + 1;
                        } else {
                            high = mid;
                        }
                    }
                    return children.get(low).get(index - (low == 0 ? 0 : ends[low - 1]));
                },
                () -> Iterators.concat(Iterators.transform(children.iterator(), List::iterator)));
    }

    public static CommitMessageList map(
            List<CommitMessage> messages, UnaryOperator<CommitMessage> mapper) {
        return new CommitMessageList(
                messages.size(),
                index -> mapper.apply(messages.get(index)),
                () -> Iterators.transform(messages.iterator(), mapper::apply));
    }

    @Override
    public int size() {
        return materialized == null ? size : materialized.size();
    }

    @Override
    public CommitMessage get(int index) {
        if (materialized != null) {
            return materialized.get(index);
        }
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Message index: " + index + ", size: " + size);
        }
        return getter.apply(index);
    }

    @Override
    public Iterator<CommitMessage> iterator() {
        return Iterators.unmodifiableIterator(
                materialized == null ? iterators.get() : materialized.iterator());
    }

    /** Restores stable, mutable objects for extensions that retain or modify commit messages. */
    public List<CommitMessage> materialize() {
        if (materialized == null) {
            materialized = new ArrayList<>(this);
        }
        return materialized;
    }

    private static CommitMessage deserialize(CommitMessageSerializer serializer, byte[] bytes) {
        try {
            return serializer.deserialize(serializer.getVersion(), bytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize commit message", e);
        }
    }
}
