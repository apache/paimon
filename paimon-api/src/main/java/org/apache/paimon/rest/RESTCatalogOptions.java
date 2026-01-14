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
            ConfigOptions.key("dlf.access-key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key id");

    public static final ConfigOption<String> DLF_ACCESS_KEY_SECRET =
            ConfigOptions.key("dlf.access-key-secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key secret");

    public static final ConfigOption<String> DLF_SECURITY_TOKEN =
            ConfigOptions.key("dlf.security-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF security token");

    public static final ConfigOption<String> DLF_TOKEN_LOADER =
            ConfigOptions.key("dlf.token-loader")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token loader.");

    public static final ConfigOption<String> DLF_TOKEN_ECS_METADATA_URL =
            ConfigOptions.key("dlf.token-ecs-metadata-url")
                    .stringType()
                    .defaultValue(
                            "http://100.100.100.200/latest/meta-data/Ram/security-credentials/")
                    .withDescription("REST Catalog auth DLF token ecs metadata url.");

    public static final ConfigOption<String> DLF_TOKEN_ECS_ROLE_NAME =
            ConfigOptions.key("dlf.token-ecs-role-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token ecs role name.");

    public static final ConfigOption<String> HTTP_USER_AGENT =
            ConfigOptions.key("header.User-Agent")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The user agent of http client connecting to REST Catalog server.");

    public static final ConfigOption<String> DLF_OSS_ENDPOINT =
            ConfigOptions.key("dlf.oss-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog DLF OSS endpoint.");

    public static final ConfigOption<Boolean> IO_CACHE_ENABLED =
            ConfigOptions.key("io-cache.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable cache for visiting files using file io (currently only JindoFileIO supports cache).");
    public static final ConfigOption<String> IO_CACHE_WHITELIST_PATH =
            ConfigOptions.key("io-cache.whitelist-path")
                    .stringType()
                    .defaultValue("bucket-,manifest")
                    .withDescription(
                            "Cache is only applied to paths which contain the specified pattern, and * means all paths.");
    public static final ConfigOption<String> IO_CACHE_POLICY =
            ConfigOptions.key("io-cache.policy")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table-level cache policy provided by the REST server, combined with: meta,read,write."
                                    + "`meta`: meta cache is enabled for visiting files; "
                                    + "`read`: cache is enabled when reading files; "
                                    + "`write`: data is also cached when writing files; "
                                    + "`none`: cache is all disabled.");
}
