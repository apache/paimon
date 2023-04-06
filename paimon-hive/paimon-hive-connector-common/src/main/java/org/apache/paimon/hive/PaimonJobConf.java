/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to convert Hive table property keys and get file store specific configurations from
 * {@link JobConf}.
 */
public class PaimonJobConf {

    private static final String INTERNAL_LOCATION = "paimon.internal.location";
    private static final String INTERNAL_CATALOG_CONFIG = "paimon.catalog.config";

    private static final String PAIMON_PREFIX = "paimon.";

    private final JobConf jobConf;

    public PaimonJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public static void configureInputJobProperties(
            Configuration configuration, Properties properties, Map<String, String> map) {
        map.put(
                INTERNAL_CATALOG_CONFIG,
                JsonSerdeUtil.toJson(extractCatalogConfig(configuration).toMap()));
        map.put(
                INTERNAL_LOCATION,
                properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION));
    }

    public String getLocation() {
        return jobConf.get(INTERNAL_LOCATION);
    }

    /** Extract paimon catalog conf from Hive conf. */
    public static Options extractCatalogConfig(Configuration hiveConf) {
        Map<String, String> configMap = new HashMap<>();

        if (hiveConf != null) {
            for (Map.Entry<String, String> entry : hiveConf) {
                String name = entry.getKey();
                if (name.startsWith(PAIMON_PREFIX)) {
                    String value = hiveConf.get(name);
                    name = name.substring(PAIMON_PREFIX.length());
                    configMap.put(name, value);
                }
            }
        }
        return Options.fromMap(configMap);
    }
}
