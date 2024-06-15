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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.utils.TimeUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.options.CatalogOptions.LOCK_CHECK_MAX_SLEEP;

/** Jdbc catalog lock. */
public class JdbcCatalogLock implements CatalogLock {
    private final JdbcClientPool connections;
    private final long checkMaxSleep;
    private final long acquireTimeout;
    private final String catalogKey;

    public JdbcCatalogLock(
            JdbcClientPool connections,
            String catalogKey,
            long checkMaxSleep,
            long acquireTimeout) {
        this.connections = connections;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
        this.catalogKey = catalogKey;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        String lockUniqueName = String.format("%s.%s.%s", catalogKey, database, table);
        lock(lockUniqueName);
        try {
            return callable.call();
        } finally {
            JdbcUtils.release(connections, lockUniqueName);
        }
    }

    private void lock(String lockUniqueName) throws SQLException, InterruptedException {
        boolean lock = JdbcUtils.acquire(connections, lockUniqueName, acquireTimeout);
        long nextSleep = 50;
        long startRetry = System.currentTimeMillis();
        while (!lock) {
            nextSleep *= 2;
            if (nextSleep > checkMaxSleep) {
                nextSleep = checkMaxSleep;
            }
            Thread.sleep(nextSleep);
            lock = JdbcUtils.acquire(connections, lockUniqueName, acquireTimeout);
            if (System.currentTimeMillis() - startRetry > acquireTimeout) {
                break;
            }
        }
        long retryDuration = System.currentTimeMillis() - startRetry;
        if (!lock) {
            throw new RuntimeException(
                    "Acquire lock failed with time: " + Duration.ofMillis(retryDuration));
        }
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }

    public static long checkMaxSleep(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    public static long acquireTimeout(Map<String, String> conf) {
        return TimeUtils.parseDuration(
                        conf.getOrDefault(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
