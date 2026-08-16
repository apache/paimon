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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** A waiter for retry. */
public class RetryWaiter {

    private final long minRetryWait;
    private final long maxRetryWait;

    public RetryWaiter(long minRetryWait, long maxRetryWait) {
        this.minRetryWait = minRetryWait;
        this.maxRetryWait = maxRetryWait;
    }

    public void retryWait(int retryCount) {
        int retryWait = (int) Math.min(minRetryWait * Math.pow(2, retryCount), maxRetryWait);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        retryWait += random.nextInt(Math.max(1, (int) (retryWait * 0.2)));
        try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }
}
