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

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;

import static org.apache.paimon.utils.FileUtils.COMMON_IO_FORK_JOIN_POOL;

/**
 * This class is a parallel execution util class, which mainly aim to process tasks parallelly with
 * memory control.
 */
public class ScanParallelExecutor {

    // reduce memory usage by batch iterable process, the cached result in memory will be queueSize
    public static <T, U> Iterable<T> parallelismBatchIterable(
            Function<List<U>, List<T>> processor, List<U> input, @Nullable Integer queueSize) {
        ForkJoinPool poolCandidate = COMMON_IO_FORK_JOIN_POOL;
        if (queueSize == null) {
            queueSize = poolCandidate.getParallelism();
        } else if (queueSize <= 0) {
            throw new NegativeArraySizeException("queue size should not be negetive");
        }

        final Queue<List<U>> stack = new ArrayDeque<>(Lists.partition(input, queueSize));
        final int settledQueueSize = queueSize;
        return () ->
                new Iterator<T>() {
                    List<T> activeList = null;
                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        advanceIfNeeded();
                        return activeList != null && index < activeList.size();
                    }

                    @Override
                    public T next() {
                        advanceIfNeeded();
                        if (activeList == null || index >= activeList.size()) {
                            throw new NoSuchElementException();
                        }
                        return activeList.get(index++);
                    }

                    private void advanceIfNeeded() {
                        while ((activeList == null || index >= activeList.size())
                                && stack.size() > 0) {
                            // reset index
                            index = 0;
                            try {
                                activeList =
                                        CompletableFuture.supplyAsync(
                                                        () -> processor.apply(stack.poll()),
                                                        getExecutePool(settledQueueSize))
                                                .get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                };
    }

    public static ForkJoinPool getExecutePool(@Nullable Integer queueSize) {
        if (queueSize == null) {
            return COMMON_IO_FORK_JOIN_POOL;
        }

        return queueSize > COMMON_IO_FORK_JOIN_POOL.getParallelism()
                ? FileUtils.getScanIoForkJoinPool(queueSize)
                : COMMON_IO_FORK_JOIN_POOL;
    }
}
