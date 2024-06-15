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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.utils.TimeUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Callable;

import static org.apache.paimon.options.CatalogOptions.LOCK_ACQUIRE_TIMEOUT;
import static org.apache.paimon.options.CatalogOptions.LOCK_CHECK_MAX_SLEEP;

/** Hive {@link CatalogLock}. */
public class HiveCatalogLock implements CatalogLock {

    static final String LOCK_IDENTIFIER = "hive";

    private final IMetaStoreClient client;
    private final long checkMaxSleep;
    private final long acquireTimeout;

    public HiveCatalogLock(IMetaStoreClient client, long checkMaxSleep, long acquireTimeout) {
        this.client = client;
        this.checkMaxSleep = checkMaxSleep;
        this.acquireTimeout = acquireTimeout;
    }

    @Override
    public <T> T runWithLock(String database, String table, Callable<T> callable) throws Exception {
        long lockId = lock(database, table);
        try {
            return callable.call();
        } finally {
            unlock(lockId);
        }
    }

    private long lock(String database, String table)
            throws UnknownHostException, TException, InterruptedException {
        final LockComponent lockComponent =
                new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, database);
        lockComponent.setTablename(table);
        lockComponent.unsetOperationType();
        final LockRequest lockRequest =
                new LockRequest(
                        Collections.singletonList(lockComponent),
                        System.getProperty("user.name"),
                        InetAddress.getLocalHost().getHostName());
        LockResponse lockResponse = this.client.lock(lockRequest);

        long nextSleep = 50;
        long startRetry = System.currentTimeMillis();
        while (lockResponse.getState() == LockState.WAITING) {
            nextSleep *= 2;
            if (nextSleep > checkMaxSleep) {
                nextSleep = checkMaxSleep;
            }
            Thread.sleep(nextSleep);

            lockResponse = client.checkLock(lockResponse.getLockid());
            if (System.currentTimeMillis() - startRetry > acquireTimeout) {
                break;
            }
        }
        long retryDuration = System.currentTimeMillis() - startRetry;

        if (lockResponse.getState() != LockState.ACQUIRED) {
            if (lockResponse.getState() == LockState.WAITING) {
                client.unlock(lockResponse.getLockid());
            }
            throw new RuntimeException(
                    "Acquire lock failed with time: " + Duration.ofMillis(retryDuration));
        }
        return lockResponse.getLockid();
    }

    private void unlock(long lockId) throws TException {
        client.unlock(lockId);
    }

    @Override
    public void close() {
        this.client.close();
    }

    public static long checkMaxSleep(HiveConf conf) {
        return TimeUtils.parseDuration(
                        conf.get(
                                LOCK_CHECK_MAX_SLEEP.key(),
                                TimeUtils.getStringInMillis(LOCK_CHECK_MAX_SLEEP.defaultValue())))
                .toMillis();
    }

    public static long acquireTimeout(HiveConf conf) {
        return TimeUtils.parseDuration(
                        conf.get(
                                LOCK_ACQUIRE_TIMEOUT.key(),
                                TimeUtils.getStringInMillis(LOCK_ACQUIRE_TIMEOUT.defaultValue())))
                .toMillis();
    }
}
