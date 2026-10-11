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

package org.apache.paimon.oss;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.util.Arrays;
import java.util.StringJoiner;

/**
 * Builds Paimon's unified OSS User-Agent {@code module(transport;features) extended}. Each part
 * comes from {@code fs.oss.user.agent.*}, falling back to the catalog-wide {@code user-agent.*}.
 */
final class OSSUserAgent {

    static final String MODULE = "fs.oss.user.agent.module";
    static final String FEATURES = "fs.oss.user.agent.features";
    static final String EXTENDED = "fs.oss.user.agent.extended";

    static final String GENERIC_MODULE = "user-agent.module";
    static final String GENERIC_FEATURES = "user-agent.features";
    static final String GENERIC_EXTENDED = "user-agent.extended";

    /** hadoop-aliyun sends this followed by {@code ", Hadoop/<version>"}. */
    static final String PREFIX = "fs.oss.user.agent.prefix";

    static final String DLF_ACCESS_TRACKING_EXTENDED_INFO = "dlf.access-tracking.extended-info";

    private static final String DEFAULT_MODULE = "Paimon/" + OSSBuildVersions.PAIMON;

    private OSSUserAgent() {}

    static String prefix(Options options) {
        String module = part(options, MODULE, GENERIC_MODULE);
        StringBuilder builder = new StringBuilder(module == null ? DEFAULT_MODULE : module);

        builder.append("(aliyun-sdk-java/").append(OSSBuildVersions.OSS_SDK);
        String features = part(options, FEATURES, GENERIC_FEATURES);
        if (features != null) {
            for (String feature : features.split("\\s+")) {
                builder.append(';').append(feature);
            }
        }
        builder.append(')');

        // Access tracking info is appended, so a user-set extended value is kept.
        String extended =
                join(
                        part(options, EXTENDED, GENERIC_EXTENDED),
                        options.get(DLF_ACCESS_TRACKING_EXTENDED_INFO));
        if (!extended.isEmpty()) {
            builder.append(' ').append(extended);
        }
        return builder.toString();
    }

    /**
     * The OSS-specific value if set, else the catalog-wide one, trimmed; null when both are blank.
     */
    private static String part(Options options, String ossKey, String genericKey) {
        for (String key : Arrays.asList(ossKey, genericKey)) {
            String value = options.get(key);
            if (!StringUtils.isNullOrWhitespaceOnly(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private static String join(String first, String second) {
        StringJoiner joiner = new StringJoiner(" ");
        for (String part : Arrays.asList(first, second)) {
            if (!StringUtils.isNullOrWhitespaceOnly(part)) {
                joiner.add(part.trim());
            }
        }
        return joiner.toString();
    }
}
