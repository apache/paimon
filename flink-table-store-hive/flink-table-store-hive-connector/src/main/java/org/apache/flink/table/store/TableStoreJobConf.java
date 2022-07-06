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

package org.apache.flink.table.store;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.Properties;

/**
 * Utility class to convert Hive table property keys and get file store specific configurations from
 * {@link JobConf}.
 */
public class TableStoreJobConf {

    private static final String INTERNAL_LOCATION = "table-store.internal.location";

    private final JobConf jobConf;

    public TableStoreJobConf(JobConf jobConf) {
        this.jobConf = jobConf;
    }

    public static void configureInputJobProperties(Properties properties, Map<String, String> map) {
        map.put(
                INTERNAL_LOCATION,
                properties.getProperty(hive_metastoreConstants.META_TABLE_LOCATION));
    }

    public String getLocation() {
        return jobConf.get(INTERNAL_LOCATION);
    }
}
