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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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

    /** An iterator which waits for its active batch to quiesce when closed. */
    public interface CloseableBatchIterator<T> extends Iterator<T>, AutoCloseable {

        @Override
        void close();
    }

    /**
     * Create a thread pool with max thread number. Inactive threads will automatically exit.
     *
     * <p>The {@link Executors#newCachedThreadPool} cannot limit max thread number. Non-core threads
     * must be used with {@link SynchronousQueue}, but synchronous queue will be blocked when there
     * is max thread number.
     */
    public static ThreadPoolExecutor createCachedThreadPool(int threadNum, String namePrefix) {
        return createCachedThreadPool(threadNum, namePrefix, new LinkedBlockingQueue<>());
    }

    /**
     * Create a thread pool with max thread number and define queue. Inactive threads will
     * automatically exit.
     */
    public static ThreadPoolExecutor createCachedThreadPool(
            int threadNum, String namePrefix, BlockingQueue<Runnable> workQueue) {
        ThreadPoolExecutor executor =
                new ThreadPoolExecutor(
                        threadNum,
                        threadNum,
                        1,
                        TimeUnit.MINUTES,
                        workQueue,
                        newDaemonThreadFactory(namePrefix));
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /** This method aims to parallel process tasks with memory control and sequentially. */
    public static <T, U> Iterable<T> sequentialBatchedExecute(
            ExecutorService executor,
            Function<U, List<T>> processor,
            List<U> input,
            int queueSize) {
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

    /**
     * Parallel processes a bounded number of inputs at a time and returns results in input order.
     *
     * <p>The caller must close the iterator to cancel unstarted tasks and wait for running tasks.
     */
    public static <T, U> CloseableBatchIterator<T> sequentialBatchedExecuteCloseable(
            ExecutorService executor,
            Function<U, List<T>> processor,
            List<U> input,
            int queueSize) {
        return sequentialBatchedExecuteCloseable(executor, processor, input.iterator(), queueSize);
    }

    /**
     * Parallel processes a lazily supplied input, keeping at most {@code queueSize} tasks in flight
     * and returning results in input order. The input is read only as slots free, so a caller that
     * discovers its work by listing storage does not have to list all of it up front.
     *
     * <p>The caller must close the iterator to stop reading input, cancel unstarted tasks and wait
     * for running tasks. An input that fails to produce its next element reports that by throwing
     * an unchecked exception, as a processor does.
     */
    public static <T, U> CloseableBatchIterator<T> sequentialBatchedExecuteCloseable(
            ExecutorService executor,
            Function<U, List<T>> processor,
            Iterator<U> input,
            int queueSize) {
        return newSequentialBatchIterator(executor, processor, input, queueSize, true);
    }

    /**
     * As {@link #sequentialBatchedExecuteCloseable}, but closing waits for a task that has already
     * started instead of interrupting it.
     *
     * <p>Use this when a task changes stored state. Interrupting a delete or a write halfway leaves
     * the caller unable to say whether it took effect, so a caller that has to know the outcome of
     * everything it handed out cannot let close cancel work that is already running.
     */
    public static <T, U> CloseableBatchIterator<T> sequentialBatchedExecuteNonCancellable(
            ExecutorService executor,
            Function<U, List<T>> processor,
            Iterator<U> input,
            int queueSize) {
        return newSequentialBatchIterator(executor, processor, input, queueSize, false);
    }

    private static <T, U> CloseableBatchIterator<T> newSequentialBatchIterator(
            ExecutorService executor,
            Function<U, List<T>> processor,
            Iterator<U> input,
            int queueSize,
            boolean cancelRunningOnClose) {
        if (queueSize <= 0) {
            throw new NegativeArraySizeException("queue size should not be negative");
        }
        return new SequentialBatchIterator<>(
                executor, processor, input, queueSize, cancelRunningOnClose);
    }

    public static <U> void randomlyOnlyExecute(
            ExecutorService executor, Consumer<U> processor, Collection<U> input) {
        awaitAllFutures(submitAllTasks(executor, processor, input));
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

    public static <U> List<Future<?>> submitAllTasks(
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
        return futures;
    }

    public static void awaitAllFutures(List<Future<?>> futures) {
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

    private static class SequentialBatchIterator<T, U> implements CloseableBatchIterator<T> {

        private final ExecutorService executor;
        private final Function<U, List<T>> processor;
        private final Iterator<U> input;
        private final int queueSize;
        private final boolean cancelRunningOnClose;
        private final Queue<BatchTask<T, U>> activeTasks = new ArrayDeque<>();

        private Iterator<T> activeResults = Collections.<T>emptyList().iterator();
        private T next;
        private boolean closed;

        private SequentialBatchIterator(
                ExecutorService executor,
                Function<U, List<T>> processor,
                Iterator<U> input,
                int queueSize,
                boolean cancelRunningOnClose) {
            this.executor = executor;
            this.processor = processor;
            this.input = input;
            this.queueSize = queueSize;
            this.cancelRunningOnClose = cancelRunningOnClose;
        }

        @Override
        public boolean hasNext() {
            if (!closed) {
                advanceIfNeeded();
            }
            return next != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T result = next;
            next = null;
            return result;
        }

        private void advanceIfNeeded() {
            while (next == null) {
                if (activeResults.hasNext()) {
                    next = activeResults.next();
                    continue;
                }
                fillWindow();
                if (activeTasks.isEmpty()) {
                    return;
                }
                BatchTask<T, U> task = activeTasks.peek();
                try {
                    List<T> results = task.result();
                    activeTasks.poll();
                    activeResults = results.iterator();
                } catch (RuntimeException | Error failure) {
                    if (task.failureReported()) {
                        activeTasks.poll();
                    }
                    throw failure;
                }
            }
        }

        /** Hands out work until the window is full or the input is exhausted. */
        private void fillWindow() {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            while (activeTasks.size() < queueSize && input.hasNext()) {
                BatchTask<T, U> task = new BatchTask<>(processor, input.next(), classLoader);
                executor.execute(task);
                activeTasks.add(task);
            }
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;

            Throwable failure = null;
            boolean interrupted = Thread.interrupted();
            for (BatchTask<T, U> task : activeTasks) {
                try {
                    if (cancelRunningOnClose) {
                        task.cancel();
                    } else {
                        task.cancelIfUnstarted();
                    }
                } catch (Throwable cleanupFailure) {
                    failure = firstOrSuppressed(cleanupFailure, failure);
                }
            }
            for (BatchTask<T, U> task : activeTasks) {
                while (true) {
                    try {
                        task.awaitCompletion();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                Throwable taskFailure = task.unreportedFailure();
                if (taskFailure != null) {
                    failure = firstOrSuppressed(taskFailure, failure);
                }
            }
            activeTasks.clear();
            activeResults = Collections.<T>emptyList().iterator();
            next = null;

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            if (failure != null) {
                throw rethrow(failure);
            }
        }
    }

    private static class BatchTask<T, U> implements Runnable {

        private static final int CREATED = 0;
        private static final int RUNNING = 1;
        private static final int CANCELLED = 2;
        private static final int FINISHED = 3;

        private final Function<U, List<T>> processor;
        private final U input;
        private final ClassLoader classLoader;
        private final CountDownLatch completion = new CountDownLatch(1);

        private int state = CREATED;
        private Thread runner;
        private List<T> result;
        private Throwable failure;
        private volatile boolean failureReported;

        private BatchTask(Function<U, List<T>> processor, U input, ClassLoader classLoader) {
            this.processor = processor;
            this.input = input;
            this.classLoader = classLoader;
        }

        @Override
        public void run() {
            synchronized (this) {
                if (state == CANCELLED) {
                    state = FINISHED;
                    completion.countDown();
                    return;
                }
                state = RUNNING;
                runner = Thread.currentThread();
            }

            Thread currentThread = Thread.currentThread();
            ClassLoader originalClassLoader = currentThread.getContextClassLoader();
            try {
                currentThread.setContextClassLoader(classLoader);
                result = processor.apply(input);
            } catch (RuntimeException | Error taskFailure) {
                failure = taskFailure;
            } finally {
                currentThread.setContextClassLoader(originalClassLoader);
                Thread.interrupted();
                synchronized (this) {
                    runner = null;
                    state = FINISHED;
                }
                completion.countDown();
            }
        }

        private synchronized void cancel() {
            if (cancelIfUnstarted()) {
                return;
            }
            if (state == RUNNING) {
                runner.interrupt();
            }
        }

        /** Gives up on a task only while it can still be given up on without interrupting it. */
        private synchronized boolean cancelIfUnstarted() {
            if (state == CREATED) {
                state = CANCELLED;
                completion.countDown();
                return true;
            }
            return false;
        }

        private List<T> result() {
            try {
                completion.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (failure != null) {
                failureReported = true;
                throw rethrow(failure);
            }
            return result;
        }

        private void awaitCompletion() throws InterruptedException {
            completion.await();
        }

        private boolean failureReported() {
            return failureReported;
        }

        private Throwable unreportedFailure() {
            return failureReported ? null : failure;
        }
    }

    private static Throwable firstOrSuppressed(Throwable newFailure, Throwable previousFailure) {
        if (previousFailure == null || previousFailure == newFailure) {
            return newFailure;
        }
        previousFailure.addSuppressed(newFailure);
        return previousFailure;
    }

    private static RuntimeException rethrow(Throwable failure) {
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        }
        return new RuntimeException(failure);
    }
}
