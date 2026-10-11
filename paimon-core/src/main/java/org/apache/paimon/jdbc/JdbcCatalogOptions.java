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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

import java.time.Duration;

/** Options for jdbc catalog. */
public final class JdbcCatalogOptions {

    public static final ConfigOption<Boolean> LOCK_ENABLED =
            ConfigOptions.key("lock.enabled")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Enable catalog locking. Defaults to enabled on object stores.");

    public static final ConfigOption<String> LOCK_TYPE =
            ConfigOptions.key("lock.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The catalog lock factory identifier. Defaults to the catalog's built-in lock factory.");

    public static final ConfigOption<Duration> LOCK_CHECK_MAX_SLEEP =
            ConfigOptions.key("lock-check-max-sleep")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(8))
                    .withDescription("The maximum sleep time when retrying to check the lock.");

    public static final ConfigOption<Duration> LOCK_ACQUIRE_TIMEOUT =
            ConfigOptions.key("lock-acquire-timeout")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(8))
                    .withDescription("The maximum time to wait for acquiring the lock.");

    public static final ConfigOption<String> CATALOG_KEY =
            ConfigOptions.key("catalog-key")
                    .stringType()
                    .defaultValue("jdbc")
                    .withDescription("Custom jdbc catalog store key.");

    public static final ConfigOption<Integer> LOCK_KEY_MAX_LENGTH =
            ConfigOptions.key("lock-key-max-length")
                    .intType()
                    .defaultValue(255)
                    .withDescription(
                            "Set the maximum length of the lock key. The 'lock-key' is composed of concatenating three fields : 'catalog-key', 'database', and 'table'.");

    private JdbcCatalogOptions() {}

    static Integer lockKeyMaxLength(Options options) {
        return options.get(LOCK_KEY_MAX_LENGTH);
    }
}
