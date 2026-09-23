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

import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Thread-safe holder for changing the read batch size at runtime.
 *
 * <p>When no batch size is set, supporting readers use their configured default. Otherwise, readers
 * snapshot {@link #batchSize()} before starting each physical batch and use that value for both the
 * logical row count and vector capacity. When the size changes, an idle reusable batch is replaced
 * at the next safe batch boundary. A physical batch that has already started, including an
 * asynchronously prefetched batch, retains its previous size and vectors.
 *
 * <p>Concurrent updates use latest-value semantics, so readers are not required to observe every
 * intermediate size.
 */
@Public
@ThreadSafe
public final class ReadBatchSizer {

    private static final int UNSET_BATCH_SIZE = 0;

    private final AtomicInteger batchSize = new AtomicInteger(UNSET_BATCH_SIZE);

    public ReadBatchSizer() {}

    /**
     * Row count and vector capacity for a future physical batch.
     *
     * <p>An empty value means that readers should use their configured default batch size. Readers
     * snapshot this value at a format-specific physical batch boundary.
     */
    public OptionalInt batchSize() {
        int value = batchSize.get();
        return value == UNSET_BATCH_SIZE ? OptionalInt.empty() : OptionalInt.of(value);
    }

    /**
     * Set the size for future physical batches.
     *
     * <p>The value must be positive. A reader that already started or prefetched a physical batch
     * may finish that batch with the previous size.
     */
    public void setBatchSize(int batchSize) {
        checkArgument(batchSize > 0, "Batch size must be positive, but was %s.", batchSize);
        this.batchSize.set(batchSize);
    }

    /** Clear the size so that readers use their configured default for future physical batches. */
    public void clearBatchSize() {
        batchSize.set(UNSET_BATCH_SIZE);
    }
}
