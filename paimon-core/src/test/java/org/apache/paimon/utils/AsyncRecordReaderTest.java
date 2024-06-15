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

import org.apache.paimon.reader.RecordReader;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AsyncRecordReader}. */
public class AsyncRecordReaderTest {

    @Test
    public void testNormal() throws IOException {
        Queue<List<Integer>> queue = new LinkedList<>();
        queue.add(Arrays.asList(1, 5, 6));
        queue.add(Arrays.asList(4, 6, 8));
        queue.add(Arrays.asList(9, 1));
        AtomicInteger released = new AtomicInteger(0);
        RecordReader<Integer> reader =
                new RecordReader<Integer>() {
                    @Nullable
                    @Override
                    public RecordIterator<Integer> readBatch() {
                        List<Integer> values = queue.poll();
                        if (values == null) {
                            return null;
                        }
                        Queue<Integer> vQueue = new LinkedList<>(values);
                        return new RecordIterator<Integer>() {
                            @Nullable
                            @Override
                            public Integer next() {
                                return vQueue.poll();
                            }

                            @Override
                            public void releaseBatch() {
                                released.incrementAndGet();
                            }
                        };
                    }

                    @Override
                    public void close() {}
                };

        AsyncRecordReader<Integer> asyncReader = new AsyncRecordReader<>(() -> reader);
        List<Integer> results = new ArrayList<>();
        asyncReader.forEachRemaining(results::add);
        assertThat(results).containsExactly(1, 5, 6, 4, 6, 8, 9, 1);
        assertThat(released.get()).isEqualTo(3);
    }

    @Test
    public void testNonBlockingWhenException() {
        String message = "Test Exception";
        RecordReader<Integer> reader =
                new RecordReader<Integer>() {
                    @Nullable
                    @Override
                    public RecordIterator<Integer> readBatch() {
                        throw new RuntimeException(message);
                    }

                    @Override
                    public void close() {}
                };

        AsyncRecordReader<Integer> asyncReader = new AsyncRecordReader<>(() -> reader);
        assertThatThrownBy(() -> asyncReader.forEachRemaining(v -> {}))
                .hasMessageContaining(message);
    }

    @Test
    public void testClassLoader() throws IOException {
        ClassLoader goodClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader badClassLoader =
                    new ClassLoader() {
                        @Override
                        public Class<?> loadClass(String name) {
                            throw new RuntimeException();
                        }
                    };
            Thread.currentThread().setContextClassLoader(badClassLoader);

            RecordReader<Integer> reader1 =
                    new RecordReader<Integer>() {
                        @Nullable
                        @Override
                        public RecordIterator<Integer> readBatch() {
                            return null;
                        }

                        @Override
                        public void close() {}
                    };

            AsyncRecordReader<Integer> asyncReader = new AsyncRecordReader<>(() -> reader1);
            asyncReader.forEachRemaining(v -> {});

            Thread.currentThread().setContextClassLoader(goodClassLoader);
            RecordReader<Integer> reader2 =
                    new RecordReader<Integer>() {
                        @Nullable
                        @Override
                        public RecordIterator<Integer> readBatch() {
                            try {
                                Thread.currentThread()
                                        .getContextClassLoader()
                                        .loadClass(AsyncRecordReaderTest.class.getName());
                            } catch (ClassNotFoundException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        }

                        @Override
                        public void close() {}
                    };

            asyncReader = new AsyncRecordReader<>(() -> reader2);
            asyncReader.forEachRemaining(v -> {});
        } finally {
            Thread.currentThread().setContextClassLoader(goodClassLoader);
        }
    }
}
