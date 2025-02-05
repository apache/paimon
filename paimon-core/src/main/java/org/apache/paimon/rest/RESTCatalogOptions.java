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

package org.apache.paimon.rest;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import java.time.Duration;

/** Options for REST Catalog. */
public class RESTCatalogOptions {

    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog server's uri.");

    public static final ConfigOption<Duration> CONNECTION_TIMEOUT =
            ConfigOptions.key("rest.client.connection-timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(180))
                    .withDescription("REST Catalog http client connect timeout.");

    public static final ConfigOption<Integer> MAX_CONNECTIONS =
            ConfigOptions.key("rest.client.max-connections")
                    .intType()
                    .defaultValue(100)
                    .withDescription("REST Catalog http client's max connections.");

    public static final ConfigOption<Integer> MAX_RETIES =
            ConfigOptions.key("rest.client.max-retries")
                    .intType()
                    .defaultValue(5)
                    .withDescription("REST Catalog http client's max retry times.");

    public static final ConfigOption<Integer> THREAD_POOL_SIZE =
            ConfigOptions.key("rest.client.num-threads")
                    .intType()
                    .defaultValue(1)
                    .withDescription("REST Catalog http client thread num.");

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token.");

    public static final ConfigOption<Duration> TOKEN_EXPIRATION_TIME =
            ConfigOptions.key("token.expiration-time")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "REST Catalog auth token expires time.The token generates system refresh frequency is t1,"
                                    + " the token expires time is t2, we need to guarantee that t2 > t1,"
                                    + " the token validity time is [t2 - t1, t2],"
                                    + " and the expires time defined here needs to be less than (t2 - t1)");

    public static final ConfigOption<String> TOKEN_PROVIDER_PATH =
            ConfigOptions.key("token.provider.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token provider path.");

    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");
}
