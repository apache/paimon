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

package org.apache.paimon.append;

import javax.annotation.Nullable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/** Bounded executor for concurrent Parquet footer metadata reads during fast-path compaction. */
public class ParquetFooterReadExecutor implements AutoCloseable {

    public static final int HARD_LIMIT = 8;

    private static final String THREAD_NAME = "paimon-footer-reader";

    private final int configuredParallelism;

    @Nullable private ThreadPoolExecutor executor;

    private boolean closed;

    public ParquetFooterReadExecutor(int configuredParallelism) {
        this.configuredParallelism = Math.max(1, Math.min(configuredParallelism, HARD_LIMIT));
    }

    public int configuredParallelism() {
        return configuredParallelism;
    }

    public int effectiveParallelism(int inputFileCount) {
        return Math.min(configuredParallelism, inputFileCount);
    }

    public boolean isConcurrentEnabled() {
        return configuredParallelism > 1;
    }

    @Nullable
    public ExecutorService executor() {
        if (!isConcurrentEnabled() || closed) {
            return null;
        }
        if (executor == null) {
            executor =
                    new ThreadPoolExecutor(
                            configuredParallelism,
                            configuredParallelism,
                            1,
                            TimeUnit.MINUTES,
                            new LinkedBlockingQueue<>(configuredParallelism),
                            newDaemonThreadFactory(THREAD_NAME),
                            new ThreadPoolExecutor.CallerRunsPolicy());
            executor.allowCoreThreadTimeOut(true);
        }
        return executor;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }
}
