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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A pool to cache and recycle heavyweight objects, to reduce object allocation. */
public class Pool<T> {

    private final ArrayBlockingQueue<T> pool;

    private final Recycler<T> recycler;

    private final int poolCapacity;
    private int poolSize;

    /**
     * Creates a pool with the given capacity. No more than that many elements may be added to the
     * pool.
     */
    public Pool(int poolCapacity) {
        this.pool = new ArrayBlockingQueue<>(poolCapacity);
        this.recycler = this::addBack;
        this.poolCapacity = poolCapacity;
        this.poolSize = 0;
    }

    /**
     * Gets the recycler for this pool. The recycler returns its given objects back to this pool.
     */
    public Recycler<T> recycler() {
        return recycler;
    }

    /**
     * Adds an entry to the pool with an optional payload. This method fails if called more often
     * than the pool capacity specified during construction.
     */
    public synchronized void add(T object) {
        if (poolSize >= poolCapacity) {
            throw new IllegalStateException("No space left in pool");
        }
        poolSize++;

        addBack(object);
    }

    /** Gets the next cached entry. This blocks until the next entry is available. */
    public T pollEntry() throws InterruptedException {
        return pool.take();
    }

    public T pollEntry(Duration timeout) throws InterruptedException {
        return pool.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Tries to get the next cached entry. If the pool is empty, this method returns null. */
    @Nullable
    public T tryPollEntry() {
        return pool.poll();
    }

    /** Internal callback to put an entry back to the pool. */
    void addBack(T object) {
        pool.add(object);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * A Recycler puts objects into the pool that the recycler is associated with.
     *
     * @param <T> The pooled and recycled type.
     */
    @FunctionalInterface
    public interface Recycler<T> {

        /** Recycles the given object to the pool that this recycler works with. */
        void recycle(T object);
    }
}
