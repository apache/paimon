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

package org.apache.paimon.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Adapter class that wraps a {@link MutableObjectIterator} into a standard Java {@link Iterator}.
 *
 * <p>This adapter handles the key differences between {@link MutableObjectIterator} and standard
 * {@link Iterator}:
 *
 * <ul>
 *   <li>{@link MutableObjectIterator} returns {@code null} to indicate the end of iteration, while
 *       {@link Iterator} uses {@link #hasNext()} and {@link #next()} separation.
 *   <li>{@link MutableObjectIterator} can throw {@link IOException} from its {@code next()} method,
 *       while standard {@link Iterator} does not declare checked exceptions. This adapter wraps
 *       {@link IOException} into {@link RuntimeException}.
 * </ul>
 *
 * @param <E> The element type of the iterator.
 */
public class MutableObjectIteratorAdapter<I extends E, E> implements Iterator<E> {

    private final MutableObjectIterator<I> delegate;
    private final I instance;

    private E nextElement;
    private boolean prefetched = false;

    /**
     * Creates a new adapter wrapping the given {@link MutableObjectIterator}.
     *
     * @param delegate The iterator to wrap.
     */
    public MutableObjectIteratorAdapter(MutableObjectIterator<I> delegate, I instance) {
        this.delegate = delegate;
        this.instance = instance;
    }

    @Override
    public boolean hasNext() {
        if (!prefetched) {
            prefetch();
        }
        return nextElement != null;
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        prefetched = false;
        return nextElement;
    }

    /** Prefetches the next element from the delegate iterator. */
    private void prefetch() {
        try {
            nextElement = delegate.next(instance);
            prefetched = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read next element from MutableObjectIterator", e);
        }
    }
}
