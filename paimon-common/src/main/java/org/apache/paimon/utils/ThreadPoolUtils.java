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

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;
import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/** Utils for thread pool. */
public class ThreadPoolUtils {

    /**
     * Create a thread pool with max thread number. Inactive threads will automatically exit.
     *
     * <p>The {@link Executors#newCachedThreadPool} cannot limit max thread number. Non-core threads
     * must be used with {@link SynchronousQueue}, but synchronous queue will be blocked when there
     * is max thread number.
     */
    public static ThreadPoolExecutor createCachedThreadPool(int threadNum, String namePrefix) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        threadNum,
                        threadNum,
                        1,
                        TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(),
                        newDaemonThreadFactory(namePrefix));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /** This method aims to parallel process tasks with memory control and sequentially. */
    public static <T, U> Iterable<T> sequentialBatchedExecute(
            ThreadPoolExecutor executor,
            Function<U, List<T>> processor,
            List<U> input,
            @Nullable Integer queueSize) {
        if (queueSize == null) {
            queueSize = executor.getMaximumPoolSize();
        }
        if (queueSize <= 0) {
            throw new NegativeArraySizeException("queue size should not be negative");
        }

        final Queue<List<U>> stack = new ArrayDeque<>(Lists.partition(input, queueSize));
        return () ->
                new Iterator<T>() {
                    Iterator<T> activeList = null;
                    T next = null;

                    @Override
                    public boolean hasNext() {
                        advanceIfNeeded();
                        return next != null;
                    }

                    @Override
                    public T next() {
                        if (next == null) {
                            throw new NoSuchElementException();
                        }

                        T result = next;
                        next = null;
                        return result;
                    }

                    private void advanceIfNeeded() {
                        while (next == null) {
                            if (activeList != null && activeList.hasNext()) {
                                next = activeList.next();
                            } else {
                                if (stack.isEmpty()) {
                                    return;
                                }
                                activeList =
                                        randomlyExecuteSequentialReturn(
                                                executor, processor, stack.poll());
                            }
                        }
                    }
                };
    }

    public static <U> void randomlyOnlyExecute(
            ExecutorService executor, Consumer<U> processor, Collection<U> input) {
        List<Future<?>> futures = new ArrayList<>(input.size());
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (U u : input) {
            futures.add(
                    executor.submit(
                            () -> {
                                Thread.currentThread().setContextClassLoader(cl);
                                processor.accept(u);
                            }));
        }
        awaitAllFutures(futures);
    }

    public static <U, T> Iterator<T> randomlyExecuteSequentialReturn(
            ExecutorService executor, Function<U, List<T>> processor, Collection<U> input) {
        List<Future<List<T>>> futures = new ArrayList<>(input.size());
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        for (U u : input) {
            futures.add(
                    executor.submit(
                            () -> {
                                Thread.currentThread().setContextClassLoader(cl);
                                return processor.apply(u);
                            }));
        }
        return futuresToIterIter(futures);
    }

    private static <T> Iterator<T> futuresToIterIter(List<Future<List<T>>> futures) {
        Queue<Future<List<T>>> queue = new ArrayDeque<>(futures);
        return Iterators.concat(
                new Iterator<Iterator<T>>() {
                    @Override
                    public boolean hasNext() {
                        return !queue.isEmpty();
                    }

                    @Override
                    public Iterator<T> next() {
                        try {
                            return queue.poll().get().iterator();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    private static void awaitAllFutures(List<Future<?>> futures) {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
