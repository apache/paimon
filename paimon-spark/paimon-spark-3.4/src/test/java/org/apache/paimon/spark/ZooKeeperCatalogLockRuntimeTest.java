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

package org.apache.paimon.spark;

import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.zookeeper.ZooKeeperCatalogLockFactory;
import org.apache.paimon.zookeeper.ZooKeeperCatalogLockOptions;

import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Exercises the lock with the Curator libraries supplied by Spark 3.4's runtime classpath. */
class ZooKeeperCatalogLockRuntimeTest {

    @Test
    void runsWithSparkCuratorClasspath() throws Exception {
        try (TestingServer server = new TestingServer(true)) {
            Options options = new Options();
            options.set(CatalogOptions.WAREHOUSE, "s3://test-bucket/warehouse");
            options.set(ZooKeeperCatalogLockOptions.QUORUM, server.getConnectString());
            options.set(ZooKeeperCatalogLockOptions.SESSION_TIMEOUT, Duration.ofSeconds(10));
            options.set(ZooKeeperCatalogLockOptions.LOCK_ACQUIRE_TIMEOUT, Duration.ofSeconds(30));

            try (Lock lock =
                    Lock.reentrant(
                            new ZooKeeperCatalogLockFactory()
                                    .createLock(
                                            CatalogLockContext.fromOptions(options),
                                            new Identifier("db", "table"),
                                            null,
                                            null,
                                            new Options()))) {
                assertEquals("committed", lock.runWithLock(() -> "committed"));
            }
        }
    }
}
