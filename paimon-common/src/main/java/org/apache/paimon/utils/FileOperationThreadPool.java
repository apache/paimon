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

import org.apache.paimon.fs.FileIO;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/** Thread pool to operate files using {@link FileIO}. */
public class FileOperationThreadPool {

    private static final String THREAD_NAME = "FILE-OPERATION-THREAD-POOL";

    private static ThreadPoolExecutor executorService =
            createCachedThreadPool(Runtime.getRuntime().availableProcessors(), THREAD_NAME);

    public static synchronized ExecutorService getExecutorService(int threadNum) {
        if (threadNum <= 0 || threadNum == executorService.getMaximumPoolSize()) {
            return executorService;
        }
        if (threadNum < executorService.getMaximumPoolSize()) {
            return new SemaphoredDelegatingExecutor(executorService, threadNum, false);
        }
        // we don't need to close previous pool
        // it is just cached pool
        executorService = createCachedThreadPool(threadNum, THREAD_NAME);

        return executorService;
    }
}
