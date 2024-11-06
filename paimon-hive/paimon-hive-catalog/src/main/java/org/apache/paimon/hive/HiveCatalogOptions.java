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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.description.Description;

import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.description.TextElement.text;

/** Options for hive catalog. */
public final class HiveCatalogOptions {

    public static final String IDENTIFIER = "hive";

    public static final ConfigOption<String> HIVE_CONF_DIR =
            ConfigOptions.key("hive-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File directory of the hive-site.xml , used to create HiveMetastoreClient and security authentication, such as Kerberos, LDAP, Ranger and so on.\n"
                                    + "If not configured, try to load from 'HIVE_CONF_DIR' env.\n");

    public static final ConfigOption<String> HADOOP_CONF_DIR =
            ConfigOptions.key("hadoop-conf-dir")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "File directory of the core-site.xml、hdfs-site.xml、yarn-site.xml、mapred-site.xml. Currently, only local file system paths are supported.\n"
                                    + "If not configured, try to load from 'HADOOP_CONF_DIR' or 'HADOOP_HOME' system environment.\n"
                                    + "Configure Priority: 1.from 'hadoop-conf-dir' 2.from HADOOP_CONF_DIR  3.from HADOOP_HOME/conf 4.HADOOP_HOME/etc/hadoop.\n");

    public static final ConfigOption<Boolean> LOCATION_IN_PROPERTIES =
            ConfigOptions.key("location-in-properties")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Setting the location in properties of hive table/database.\n"
                                    + "If you don't want to access the location by the filesystem of hive when using a object storage such as s3,oss\n"
                                    + "you can set this option to true.\n");

    public static final ConfigOption<Long> CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
            ConfigOptions.key("client-pool-cache.eviction-interval-ms")
                    .longType()
                    .defaultValue(TimeUnit.MINUTES.toMillis(5))
                    .withDescription("Setting the client's pool cache eviction interval(ms).\n");

    public static final ConfigOption<String> CLIENT_POOL_CACHE_KEYS =
            ConfigOptions.key("client-pool-cache.keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specify client cache key, multiple elements separated by commas.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "\"ugi\":  the Hadoop UserGroupInformation instance that represents the current user using the cache."))
                                    .list(
                                            text(
                                                    "\"user_name\" similar to UGI but only includes the user's name determined by UserGroupInformation#getUserName."))
                                    .list(
                                            text(
                                                    "\"conf\": name of an arbitrary configuration. "
                                                            + "The value of the configuration will be extracted from catalog properties and added to the cache key. A conf element should start with a \"conf:\" prefix which is followed by the configuration name. "
                                                            + "E.g. specifying \"conf:a.b.c\" will add \"a.b.c\" to the key, and so that configurations with different default catalog wouldn't share the same client pool. Multiple conf elements can be specified."))
                                    .build());

    public static final ConfigOption<Boolean> FORMAT_TABLE_ENABLED =
            ConfigOptions.key("format-table.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to support format tables, format table corresponds to a regular Hive table, allowing read and write operations. "
                                    + "However, during these processes, it does not connect to the metastore; hence, newly added partitions will not be reflected in"
                                    + " the metastore and need to be manually added as separate partition operations.");

    private HiveCatalogOptions() {}
}
