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

package org.apache.paimon.flink.utils;

import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods about Flink {@link ChangelogMode} to resolve compatibility issues. */
public class ChangelogModeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogModeUtils.class);

    public static ChangelogMode.Builder enableKeyOnlyDeletes(ChangelogMode.Builder builder) {
        if (isFlink21OrAbove()) {
            return builder.keyOnlyDeletes(true);
        }
        LOG.warn(
                "'sink.key-only-deletes.enabled' requires Flink 2.1+ (current version: {}); "
                        + "key-only deletes are not enabled.",
                EnvironmentInformation.getVersion());
        return builder;
    }

    private static boolean isFlink21OrAbove() {
        String version = EnvironmentInformation.getVersion();
        try {
            String[] parts = version.split("\\.");
            int major = Integer.parseInt(parts[0]);
            int minor = Integer.parseInt(parts[1]);
            return major > 2 || (major == 2 && minor >= 1);
        } catch (RuntimeException e) {
            LOG.warn(
                    "Could not parse Flink version '{}'; key-only deletes are not enabled.",
                    version);
            return false;
        }
    }
}
