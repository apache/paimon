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

package org.apache.paimon.flink.source.align;

import org.apache.flink.runtime.io.AvailabilityProvider;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple implementation of <code>FutureCompletingBlockingDeque</code>, which refers to {@link
 * org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue}.
 */
public class FutureCompletingBlockingDeque<T> {

    public static final CompletableFuture<Void> AVAILABLE =
            (CompletableFuture<Void>) AvailabilityProvider.AVAILABLE;

    /** The element queue. */
    @GuardedBy("lock")
    private final Deque<T> queue;

    /**
     * The availability future. This doubles as a "non empty" condition. This value is never null.
     */
    private CompletableFuture<Void> currentFuture;

    /** The lock for synchronization. */
    private final Lock lock;

    public FutureCompletingBlockingDeque() {
        this.queue = new ArrayDeque<>();
        this.lock = new ReentrantLock();

        // initially the queue is empty and thus unavailable
        this.currentFuture = new CompletableFuture<>();
    }

    /**
     * Get the last element from the queue without removing it.
     *
     * @return the last element in the queue, or Null if the queue is empty.
     */
    public T peekLast() {
        lock.lock();
        try {
            return queue.peekLast();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get and remove the first element from the queue.
     *
     * @return the first element from the queue, or Null if the queue is empty.
     */
    public T poll() {
        lock.lock();
        try {
            final T element = queue.poll();
            if (queue.size() == 0) {
                moveToUnAvailable();
            }
            return element;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Put an element into the queue.
     *
     * @param element the element to put.
     * @throws InterruptedException when the thread is interrupted.
     */
    public void put(T element) {
        if (element == null) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            final int sizeBefore = queue.size();
            queue.add(element);
            if (sizeBefore == 0) {
                moveToAvailable();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Put all elements into the queue.
     *
     * @param elements the elements to put.
     * @throws InterruptedException when the thread is interrupted.
     */
    public void putAll(Collection<T> elements) {
        if (elements == null) {
            throw new NullPointerException();
        }
        lock.lock();
        try {
            final int sizeBefore = queue.size();
            queue.addAll(elements);
            if (sizeBefore == 0 && elements.size() > 0) {
                moveToAvailable();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Gets the size of the queue. */
    public int size() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    /** Checks whether the queue is empty. */
    public boolean isEmpty() {
        lock.lock();
        try {
            return queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /** Returns all the elements in this queue. */
    public List<T> remainingElements() {
        lock.lock();
        try {
            return new ArrayList<>(queue);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the availability future. If the queue is non-empty, then this future will already be
     * complete. Otherwise the obtained future is guaranteed to get completed the next time the
     * queue becomes non-empty.
     */
    public CompletableFuture<Void> getAvailabilityFuture() {
        return currentFuture;
    }

    /**
     * Makes sure the availability future is complete, if it is not complete already. All futures
     * returned by previous calls to {@link #getAvailabilityFuture()} are guaranteed to be
     * completed.
     *
     * <p>All future calls to the method will return a completed future, until the point that the
     * availability is reset via calls to {@link #poll()} that leave the queue empty.
     */
    public void notifyAvailable() {
        lock.lock();
        try {
            moveToAvailable();
        } finally {
            lock.unlock();
        }
    }

    /** Internal utility to make sure that the current future futures are complete (until reset). */
    @GuardedBy("lock")
    private void moveToAvailable() {
        final CompletableFuture<Void> current = currentFuture;
        if (current != AVAILABLE) {
            currentFuture = AVAILABLE;
            current.complete(null);
        }
    }

    /** Makes sure the availability future is incomplete, if it was complete before. */
    @GuardedBy("lock")
    private void moveToUnAvailable() {
        if (currentFuture == AVAILABLE) {
            currentFuture = new CompletableFuture<>();
        }
    }
}
