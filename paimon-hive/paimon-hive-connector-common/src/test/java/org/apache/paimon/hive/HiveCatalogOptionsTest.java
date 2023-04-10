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

package org.apache.paimon.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for HiveCatalogOptions. */
public class HiveCatalogOptionsTest {

    public static final String HADOOP_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hadoop-conf-dir").getPath();

    public static final String HIVE_CONF_DIR =
            Thread.currentThread().getContextClassLoader().getResource("hive-conf-dir").getPath();

    @Test
    public void testHadoopConfDir() {
        HiveConf hiveConf = HiveCatalog.createHiveConf(null, HADOOP_CONF_DIR);
        assertThat(hiveConf.get("fs.defaultFS")).isEqualTo("dummy-fs");
    }

    @Test
    public void testHiveConfDir() {
        HiveConf hiveConf = HiveCatalog.createHiveConf(HIVE_CONF_DIR, null);
        assertThat(hiveConf.get("hive.metastore.uris")).isEqualTo("dummy-hms");
    }
}
