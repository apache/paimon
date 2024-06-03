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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/** A executor wrapper to execute with {@link Semaphore}. */
public class BlockingExecutor {

    private final Semaphore semaphore;
    private final ExecutorService executor;

    public BlockingExecutor(ExecutorService executor, int permits) {
        this.semaphore = new Semaphore(permits, true);
        this.executor = executor;
    }

    public void submit(Runnable task) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        executor.submit(
                () -> {
                    try {
                        task.run();
                    } finally {
                        semaphore.release();
                    }
                });
    }
}
