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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MutableObjectIteratorAdapter}. */
class MutableObjectIteratorAdapterTest {

    @Test
    public void testBasicIteration() {
        List<Integer> values = Arrays.asList(1, 2, 3, 4, 5);
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        List<Integer> result = new ArrayList<>();
        while (adapter.hasNext()) {
            result.add(adapter.next());
        }

        assertThat(result).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testEmptyIterator() {
        List<Integer> values = Collections.emptyList();
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        assertThat(adapter.hasNext()).isFalse();
        assertThatThrownBy(adapter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testSingleElement() {
        List<Integer> values = Collections.singletonList(42);
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo(42);
        assertThat(adapter.hasNext()).isFalse();
    }

    @Test
    public void testMultipleHasNextCalls() {
        List<Integer> values = Arrays.asList(1, 2);
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        // Call hasNext multiple times should not advance the iterator
        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.hasNext()).isTrue();

        assertThat(adapter.next()).isEqualTo(1);

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.hasNext()).isTrue();

        assertThat(adapter.next()).isEqualTo(2);
        assertThat(adapter.hasNext()).isFalse();
    }

    @Test
    public void testNextWithoutHasNext() {
        List<Integer> values = Arrays.asList(1, 2, 3);
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        // Call next() without calling hasNext() should still work
        assertThat(adapter.next()).isEqualTo(1);
        assertThat(adapter.next()).isEqualTo(2);
        assertThat(adapter.next()).isEqualTo(3);

        assertThatThrownBy(adapter::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    public void testIOExceptionHandling() {
        MutableObjectIterator<Integer> failingDelegate =
                new MutableObjectIterator<Integer>() {
                    private int count = 0;

                    @Override
                    public Integer next(Integer reuse) throws IOException {
                        count++;
                        if (count == 1) {
                            return 1;
                        }
                        throw new IOException("Test exception");
                    }

                    @Override
                    public Integer next() throws IOException {
                        return next(null);
                    }
                };

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(failingDelegate, 0);

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo(1);

        assertThatThrownBy(adapter::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to read next element from MutableObjectIterator")
                .hasCauseExactlyInstanceOf(IOException.class);
    }

    @Test
    public void testNullElements() {
        List<Integer> values = Arrays.asList(1, null, 3);
        MutableObjectIterator<Integer> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Integer, Integer> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0);

        assertThat(adapter.hasNext()).isTrue();
        assertThat(adapter.next()).isEqualTo(1);

        // Null element should be treated as end of iteration
        assertThat(adapter.hasNext()).isFalse();
    }

    @Test
    public void testStringType() {
        List<String> values = Arrays.asList("hello", "world", "test");
        MutableObjectIterator<String> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<String, String> adapter =
                new MutableObjectIteratorAdapter<>(delegate, "");

        List<String> result = new ArrayList<>();
        while (adapter.hasNext()) {
            result.add(adapter.next());
        }

        assertThat(result).containsExactly("hello", "world", "test");
    }

    @Test
    public void testLongType() {
        List<Long> values = Arrays.asList(100L, 200L, 300L);
        MutableObjectIterator<Long> delegate = createTestIterator(values);

        MutableObjectIteratorAdapter<Long, Long> adapter =
                new MutableObjectIteratorAdapter<>(delegate, 0L);

        List<Long> result = new ArrayList<>();
        while (adapter.hasNext()) {
            result.add(adapter.next());
        }

        assertThat(result).containsExactly(100L, 200L, 300L);
    }

    /**
     * Creates a test implementation of MutableObjectIterator that returns elements from the given
     * list.
     */
    private <T> MutableObjectIterator<T> createTestIterator(List<T> values) {
        return new MutableObjectIterator<T>() {
            private int index = 0;

            @Override
            public T next(T reuse) {
                if (index >= values.size()) {
                    return null;
                }
                return values.get(index++);
            }

            @Override
            public T next() {
                return next(null);
            }
        };
    }
}
