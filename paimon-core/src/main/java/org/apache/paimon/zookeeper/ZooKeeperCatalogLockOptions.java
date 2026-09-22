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

package org.apache.paimon.zookeeper;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import java.time.Duration;

/** ZooKeeper catalog lock options. */
public final class ZooKeeperCatalogLockOptions {

    private ZooKeeperCatalogLockOptions() {}

    public static final ConfigOption<String> QUORUM =
            ConfigOptions.key("lock.zookeeper.quorum")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "ZooKeeper connect string, e.g. 'host1:2181,host2:2181'. Required.");

    public static final ConfigOption<Duration> SESSION_TIMEOUT =
            ConfigOptions.key("lock.zookeeper.session-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("ZooKeeper session timeout.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("lock.zookeeper.connection-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(15))
                    .withDescription("ZooKeeper connection timeout.");

    public static final ConfigOption<Duration> RETRY_BASE_SLEEP =
            ConfigOptions.key("lock.zookeeper.retry-base-sleep")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Initial backoff between ZooKeeper connection retries.");

    public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
            ConfigOptions.key("lock.zookeeper.retry-max-attempts")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Maximum ZooKeeper connection retry attempts.");

    public static final ConfigOption<String> ROOT_PATH =
            ConfigOptions.key("lock.zookeeper.root")
                    .stringType()
                    .defaultValue("paimon/locks")
                    .withDescription("Root znode namespace for Paimon catalog locks.");
}
