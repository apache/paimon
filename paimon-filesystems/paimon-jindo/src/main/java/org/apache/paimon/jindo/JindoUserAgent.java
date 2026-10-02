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

package org.apache.paimon.jindo;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fills Paimon's unified User-Agent {@code module(transport;features) extended} into the {@code
 * fs.<scheme>.user.agent.*} keys, from which Jindo builds the header.
 */
final class JindoUserAgent {

    static final String MODULE = "user-agent.module";
    static final String FEATURES = "user-agent.features";
    static final String EXTENDED = "user-agent.extended";
    static final String DLF_ACCESS_TRACKING_EXTENDED_INFO = "dlf.access-tracking.extended-info";

    static final String PAIMON = "Paimon/" + JindoBuildVersions.PAIMON;

    private static final String[] SCHEMES = {"oss", "dls"};

    private JindoUserAgent() {}

    /**
     * Per part, {@code fs.<scheme>.user.agent.*} wins over the catalog-wide {@code user-agent.*}.
     */
    static void apply(Options catalogOptions, Options hadoopOptions) {
        for (String scheme : SCHEMES) {
            String prefix = "fs." + scheme + ".user.agent.";
            String module = first(hadoopOptions.get(prefix + "module"), catalogOptions.get(MODULE));
            if (module != null) {
                hadoopOptions.set(prefix + "module", module);
            }
            hadoopOptions.set(
                    prefix + "features",
                    features(
                            first(
                                    hadoopOptions.get(prefix + "features"),
                                    catalogOptions.get(FEATURES))));
            // Access tracking info is appended, so a user-set extended value is kept.
            String extended =
                    join(
                            first(
                                    hadoopOptions.get(prefix + "extended"),
                                    catalogOptions.get(EXTENDED)),
                            catalogOptions.get(DLF_ACCESS_TRACKING_EXTENDED_INFO));
            if (!extended.isEmpty()) {
                hadoopOptions.set(prefix + "extended", extended);
            }
        }
    }

    private static String features(String configured) {
        List<String> features = new ArrayList<>();
        if (configured != null) {
            features.addAll(Arrays.asList(configured.split("\\s+")));
        }
        if (features.stream().noneMatch(f -> f.equals("Paimon") || f.startsWith("Paimon/"))) {
            features.add(0, PAIMON);
        }
        return String.join(" ", features);
    }

    private static String first(String preferred, String fallback) {
        if (!StringUtils.isNullOrWhitespaceOnly(preferred)) {
            return preferred.trim();
        }
        return StringUtils.isNullOrWhitespaceOnly(fallback) ? null : fallback.trim();
    }

    private static String join(String first, String second) {
        List<String> parts = new ArrayList<>();
        for (String part : Arrays.asList(first, second)) {
            if (!StringUtils.isNullOrWhitespaceOnly(part)) {
                parts.add(part.trim());
            }
        }
        return String.join(" ", parts);
    }
}
