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

package org.apache.flink.table.store.spark;

import org.apache.spark.sql.sources.v2.DataSourceOptions;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This util convert lowercase key to case-sensitive key. The reason is that during {@link
 * SparkSource} initialization, {@link
 * org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils} puts all configuration to a
 * {@link org.apache.spark.sql.catalyst.util.CaseInsensitiveMap}. However, {@code
 * org.apache.hadoop.fs.aliyun.oss.Constants} maintains case-sensitive keys to initialize the
 * Hadoop-Oss FileSystem, which needs to be converted back.
 */
public class SparkCaseSensitiveConverter {

    private static final Set<String> CASE_SENSITIVE_KEYS = new HashSet<>();

    // OSS access verification
    private static final String ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String SECURITY_TOKEN = "fs.oss.securityToken";

    static {
        CASE_SENSITIVE_KEYS.add(ACCESS_KEY_ID);
        CASE_SENSITIVE_KEYS.add(ACCESS_KEY_SECRET);
        CASE_SENSITIVE_KEYS.add(SECURITY_TOKEN);
    }

    public static Map<String, String> convert(DataSourceOptions options) {
        Map<String, String> newOptions = new HashMap<>(options.asMap());
        CASE_SENSITIVE_KEYS.forEach(
                key -> {
                    String lowercaseKey = key.toLowerCase();
                    if (newOptions.containsKey(lowercaseKey)) {
                        newOptions.put(key, newOptions.remove(lowercaseKey));
                    }
                });
        return newOptions;
    }
}
