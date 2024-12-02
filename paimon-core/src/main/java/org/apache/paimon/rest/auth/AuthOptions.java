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

package org.apache.paimon.rest.auth;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

import java.time.Duration;

/** Options for REST Catalog Auth. */
public class AuthOptions {
    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token.");
    public static final ConfigOption<Duration> TOKEN_EXPIRES_IN =
            ConfigOptions.key("token-expires-in")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("REST Catalog auth token expires in.");
    public static final ConfigOption<Boolean> TOKEN_REFRESH_ENABLED =
            ConfigOptions.key("token-refresh-enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("REST Catalog auth token refresh enable.");
    public static final ConfigOption<String> TOKEN_FILE_PATH =
            ConfigOptions.key("token-file-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token file path.");
}
