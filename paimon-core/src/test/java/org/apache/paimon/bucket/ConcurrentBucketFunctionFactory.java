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

package org.apache.paimon.bucket;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** A deterministic bucket provider which detects concurrent use of a function instance. */
public class ConcurrentBucketFunctionFactory implements BucketFunctionFactory {

    public static final String IDENTIFIER = "test-concurrent-bucket";
    public static final String CHECK_CONCURRENCY = "test.bucket-function.check-concurrency";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public BucketFunction create(RowType bucketKeyType, Options tableOptions) {
        if (!"true".equals(tableOptions.toMap().get(CHECK_CONCURRENCY))) {
            return (row, numBuckets) -> numBuckets - 1;
        }
        AtomicBoolean active = new AtomicBoolean();
        AtomicBoolean firstCall = new AtomicBoolean(true);
        CountDownLatch overlap = new CountDownLatch(1);
        return (row, numBuckets) -> {
            if (!active.compareAndSet(false, true)) {
                overlap.countDown();
                throw new IllegalStateException("Concurrent bucket function invocation");
            }
            try {
                if (firstCall.getAndSet(false)) {
                    // Give another manifest worker time to request a different bucket count.
                    // Correctly serialized callers cannot enter until this bounded wait ends.
                    overlap.await(1, TimeUnit.SECONDS);
                }
                return numBuckets - 1;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } finally {
                active.set(false);
            }
        };
    }
}
