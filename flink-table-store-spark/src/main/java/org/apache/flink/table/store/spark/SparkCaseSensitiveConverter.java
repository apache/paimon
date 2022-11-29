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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This util convert lowercase key to case-sensitive key. The reason is that during {@link
 * SparkCatalog} initialization, {@link org.apache.spark.sql.connector.catalog.Catalogs} puts all
 * configuration to a {@link org.apache.spark.sql.util.CaseInsensitiveStringMap}. However, {@code
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

    public static Map<String, String> convert(Map<String, String> caseInsensitiveOptions) {
        Map<String, String> options = new HashMap<>(caseInsensitiveOptions);
        CASE_SENSITIVE_KEYS.forEach(
                key -> {
                    String lowercaseKey = key.toLowerCase();
                    if (caseInsensitiveOptions.containsKey(lowercaseKey)) {
                        options.put(key, options.remove(lowercaseKey));
                    }
                });
        return options;
    }

    private SparkCaseSensitiveConverter() {}
}
