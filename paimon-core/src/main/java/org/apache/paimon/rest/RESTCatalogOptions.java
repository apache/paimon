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

    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth bear token.");

    public static final ConfigOption<Duration> TOKEN_REFRESH_TIME =
            ConfigOptions.key("token.refresh-time")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("REST Catalog auth token refresh time.");

    public static final ConfigOption<String> TOKEN_PROVIDER =
            ConfigOptions.key("token.provider")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token provider.");

    public static final ConfigOption<String> DLF_REGION =
            ConfigOptions.key("dlf.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF region.");

    public static final ConfigOption<String> DLF_TOKEN_PATH =
            ConfigOptions.key("dlf.token-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token file path.");

    public static final ConfigOption<String> DLF_ACCESS_KEY_ID =
            ConfigOptions.key("dlf.accessKeyId")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key id");

    public static final ConfigOption<String> DLF_ACCESS_KEY_SECRET =
            ConfigOptions.key("dlf.accessKeySecret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key secret");

    public static final ConfigOption<String> DLF_SECURITY_TOKEN =
            ConfigOptions.key("dlf.securityToken")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF security token");

    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");
}
