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

package org.apache.paimon.s3;

import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.impl.ClientManager;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link S3AFileSystem} that evicts itself from {@link S3FileIO}'s cache when the sync Apache HTTP
 * connection pool has been shut down.
 *
 * <p>Only handles {@code IllegalStateException("Connection pool shut down")} from the Apache HTTP
 * client. Netty async failures are out of scope. Failed requests are not retried automatically;
 * callers must retry and obtain a recovered filesystem through {@link S3FileIO}.
 *
 * <p>Instances are published into {@link S3FileIO}'s static CACHE and may be shared by multiple
 * {@code S3FileIO} instances (and their per-instance {@code fsMap} entries). Recovery eviction only
 * marks this instance unusable and removes it from CACHE when the mapping still points at {@code
 * this}; it does not {@code close()} a previously published filesystem. Closing a shared published
 * instance requires additional cache-level lifecycle coordination (for example reference counting)
 * and is out of scope for this recovery path.
 */
class RecoverableS3AFileSystem extends S3AFileSystem {

    private static final String CONNECTION_POOL_SHUT_DOWN = "Connection pool shut down";

    private final AtomicBoolean usable = new AtomicBoolean(true);

    private Runnable evictFromCache;

    void bind(Runnable evictFromCache) {
        this.evictFromCache = evictFromCache;
    }

    boolean isUsable() {
        return usable.get();
    }

    /**
     * Marks this instance unusable and removes it from the shared cache only when the cache still
     * points at {@code this}.
     */
    void markUnusableAndEvict() {
        if (usable.compareAndSet(true, false) && evictFromCache != null) {
            evictFromCache.run();
        }
    }

    void handleExecutionFailure(Throwable failure) {
        if (isApachePoolShutdown(failure)) {
            markUnusableAndEvict();
        }
    }

    static boolean isApachePoolShutdown(Throwable failure) {
        Set<Throwable> visited =
                Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
        for (Throwable t = failure; t != null && visited.add(t); t = t.getCause()) {
            if (t instanceof IllegalStateException
                    && CONNECTION_POOL_SHUT_DOWN.equals(t.getMessage())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected ClientManager createClientManager(
            S3ClientFactory clientFactory,
            S3ClientFactory unencryptedClientFactory,
            S3ClientFactory.S3ClientCreationParameters clientCreationParameters,
            DurationTrackerFactory durationTrackerFactory) {
        List<ExecutionInterceptor> merged = new ArrayList<>();
        List<ExecutionInterceptor> existing = clientCreationParameters.getExecutionInterceptors();
        if (existing != null) {
            merged.addAll(existing);
        }
        merged.add(new PoolShutdownInterceptor(this));
        clientCreationParameters.withExecutionInterceptors(merged);
        return super.createClientManager(
                clientFactory,
                unencryptedClientFactory,
                clientCreationParameters,
                durationTrackerFactory);
    }

    private static final class PoolShutdownInterceptor implements ExecutionInterceptor {

        private final RecoverableS3AFileSystem owner;

        private PoolShutdownInterceptor(RecoverableS3AFileSystem owner) {
            this.owner = owner;
        }

        @Override
        public void onExecutionFailure(
                Context.FailedExecution context, ExecutionAttributes executionAttributes) {
            owner.handleExecutionFailure(context.exception());
        }
    }
}
