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

/** Options for REST Catalog. */
public class RESTCatalogOptions {
    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("rest.catalog.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog server's endpoint.");
    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("rest.catalog.token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog server's auth token.");
    public static final ConfigOption<Integer> CONNECT_TIMEOUT_MILLIS =
            ConfigOptions.key("rest.catalog.connect.timeout.millis")
                    .intType()
                    .defaultValue(3_000)
                    .withDescription("REST Catalog server connect timeout in mills.");
    public static final ConfigOption<Integer> READ_TIMEOUT_MILLIS =
            ConfigOptions.key("rest.catalog.read.timeout.millis")
                    .intType()
                    .defaultValue(3_000)
                    .withDescription("REST Catalog server read timeout in mills.");
    public static final ConfigOption<Integer> THREAD_POOL_SIZE =
            ConfigOptions.key("rest.catalog.thread.size")
                    .intType()
                    .defaultValue(1)
                    .withDescription("REST Catalog server thread size.");
}
