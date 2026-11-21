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

import java.util.AbstractSequentialList;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Read-only, repeatable traversal of commit entries without caching them. Forward traversal is
 * linear; random access and backwards traversal replay the input. The supplier must return the same
 * ordered values on every traversal and must not require closing external resources.
 */
class ReplayableList<T> extends AbstractSequentialList<T> {

    private final int size;
    private final Supplier<Iterator<T>> iterators;

    ReplayableList(int size, Supplier<Iterator<T>> iterators) {
        this.size = size;
        this.iterators = iterators;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException("Entry index: " + index + ", size: " + size);
        }
        return new ListIterator<T>() {
            private int position = index;
            private Iterator<T> forward;

            @Override
            public boolean hasNext() {
                return position < size;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (forward == null) {
                    forward = iterators.get();
                    for (int i = 0; i < position; i++) {
                        forward.next();
                    }
                }
                T result = forward.next();
                position++;
                return result;
            }

            @Override
            public boolean hasPrevious() {
                return position > 0;
            }

            @Override
            public T previous() {
                if (!hasPrevious()) {
                    throw new NoSuchElementException();
                }
                forward = null;
                return ReplayableList.this.get(--position);
            }

            @Override
            public int nextIndex() {
                return position;
            }

            @Override
            public int previousIndex() {
                return position - 1;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void set(T value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void add(T value) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
