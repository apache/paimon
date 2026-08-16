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

package org.apache.paimon.reader;

import org.apache.paimon.annotation.Public;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Thread-safe controller for changing the requested read batch size within a fixed maximum.
 *
 * <p>Supporting readers snapshot {@link #requestedBatchSize()} before starting each physical batch
 * and use that value for both the logical row count and vector capacity. When the requested size
 * changes, an idle reusable batch is replaced at the next safe batch boundary. A physical batch
 * that has already started, including an asynchronously prefetched batch, retains its previous size
 * and vectors.
 *
 * <p>{@link #maxBatchSize()} is a validation limit rather than a preallocated vector capacity.
 * Concurrent updates use latest-value semantics, so readers are not required to observe every
 * intermediate requested size.
 */
@Public
@ThreadSafe
public final class ReadBatchSizeController {

    private final int maxBatchSize;
    private final AtomicInteger requestedBatchSize;

    public ReadBatchSizeController(int maxBatchSize, int requestedBatchSize) {
        checkArgument(maxBatchSize > 0, "Maximum batch size must be positive.");
        checkRequestedBatchSize(maxBatchSize, requestedBatchSize);
        this.maxBatchSize = maxBatchSize;
        this.requestedBatchSize = new AtomicInteger(requestedBatchSize);
    }

    /** Maximum permitted requested batch size. */
    public int maxBatchSize() {
        return maxBatchSize;
    }

    /** Requested row count and vector capacity for a future physical batch. */
    public int requestedBatchSize() {
        return requestedBatchSize.get();
    }

    /** Set the requested size for future physical batches. */
    public void setRequestedBatchSize(int requestedBatchSize) {
        checkRequestedBatchSize(maxBatchSize, requestedBatchSize);
        this.requestedBatchSize.set(requestedBatchSize);
    }

    private static void checkRequestedBatchSize(int maxBatchSize, int requestedBatchSize) {
        checkArgument(
                requestedBatchSize > 0 && requestedBatchSize <= maxBatchSize,
                "Requested batch size must be between 1 and %s, but was %s.",
                maxBatchSize,
                requestedBatchSize);
    }
}
