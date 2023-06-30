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

package org.apache.paimon.utils;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.description.TextElement.text;

/**
 * Utility class for working with Hadoop-related classes. This should only be used if Hadoop is on
 * the classpath. Note: decoupled from specific engines.
 */
public class HadoopUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

    public static final ConfigOption<HadoopConfigLoader> HADOOP_CONF_LOADER =
            key("hadoop-conf-loader")
                    .enumType(HadoopConfigLoader.class)
                    .defaultValue(HadoopConfigLoader.ALL)
                    .withDescription("Specifies the way of loading hadoop config.");

    private static final String[] CONFIG_PREFIXES = {"hadoop."};
    public static final String HADOOP_HOME_ENV = "HADOOP_HOME";
    public static final String HADOOP_CONF_ENV = "HADOOP_CONF_DIR";

    /** Path to Hadoop configuration. */
    public static final String PATH_HADOOP_CONFIG = "hadoop-conf-dir";

    public static Configuration getHadoopConfiguration(Options options) {

        // Instantiate an HdfsConfiguration to load the hdfs-site.xml and hdfs-default.xml
        // from the classpath

        Configuration result = new HdfsConfiguration();
        boolean foundHadoopConfiguration = false;

        // We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
        // the hdfs configuration.
        // The properties of a newly added resource will override the ones in previous resources, so
        // a configuration
        // file with higher priority should be added later.

        // Approach 1: HADOOP_HOME environment variables
        String[] possibleHadoopConfPaths = new String[2];

        HadoopConfigLoader loader = options.get(HADOOP_CONF_LOADER);

        final String hadoopHomeDir = System.getenv(HADOOP_HOME_ENV);
        if (hadoopHomeDir != null && loader.loadEnv()) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_HOME: {}", hadoopHomeDir);
            possibleHadoopConfPaths[0] = hadoopHomeDir + "/conf";
            possibleHadoopConfPaths[1] = hadoopHomeDir + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                foundHadoopConfiguration = addHadoopConfIfFound(result, possibleHadoopConfPath);
            }
        }

        // Approach 2: Paimon Catalog Option
        final String hadoopConfigPath = options.getString(PATH_HADOOP_CONFIG, null);
        if (hadoopConfigPath != null && loader.loadOption()) {
            LOG.debug(
                    "Searching Hadoop configuration files in Paimon config: {}", hadoopConfigPath);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfigPath) || foundHadoopConfiguration;
        }

        // Approach 3: HADOOP_CONF_DIR environment variable
        String hadoopConfDir = System.getenv(HADOOP_CONF_ENV);
        if (hadoopConfDir != null && loader.loadEnv()) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_CONF_DIR: {}", hadoopConfDir);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfDir) || foundHadoopConfiguration;
        }

        // Approach 4: Paimon configuration
        // add all configuration key with prefix 'hadoop.' in Paimon conf to hadoop conf
        for (String key : options.keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String newKey = key.substring(prefix.length());
                    String value = options.getString(key, null);
                    result.set(newKey, value);
                    LOG.debug(
                            "Adding Paimon config entry for {} as {}={} to Hadoop config",
                            key,
                            newKey,
                            value);
                    foundHadoopConfiguration = true;
                }
            }
        }

        if (!foundHadoopConfiguration) {
            LOG.warn("Could not find Hadoop configuration via any of the supported methods");
        }

        return result;
    }

    /**
     * Search Hadoop configuration files in the given path, and add them to the configuration if
     * found.
     */
    private static boolean addHadoopConfIfFound(
            Configuration configuration, String possibleHadoopConfPath) {
        boolean foundHadoopConfiguration = false;
        if (new File(possibleHadoopConfPath).exists()) {
            if (new File(possibleHadoopConfPath + "/core-site.xml").exists()) {
                configuration.addResource(
                        new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/core-site.xml"));
                LOG.debug(
                        "Adding "
                                + possibleHadoopConfPath
                                + "/core-site.xml to hadoop configuration");
                foundHadoopConfiguration = true;
            }
            if (new File(possibleHadoopConfPath + "/hdfs-site.xml").exists()) {
                configuration.addResource(
                        new org.apache.hadoop.fs.Path(possibleHadoopConfPath + "/hdfs-site.xml"));
                LOG.debug(
                        "Adding "
                                + possibleHadoopConfPath
                                + "/hdfs-site.xml to hadoop configuration");
                foundHadoopConfiguration = true;
            }
        }
        return foundHadoopConfiguration;
    }

    /** Specifies the way of loading hadoop config. */
    public enum HadoopConfigLoader implements DescribedEnum {
        ALL("all", "Load Hadoop conf from environment variables and catalog option.", true, true),
        ENV("env", "Load Hadoop conf from environment variables only.", true, false),
        OPTION("option", "Load Hadoop conf from catalog option only.", false, true);

        private final String value;
        private final String description;
        private final boolean loadEnv;
        private final boolean loadOption;

        HadoopConfigLoader(String value, String description, boolean loadEnv, boolean loadOption) {
            this.value = value;
            this.description = description;
            this.loadEnv = loadEnv;
            this.loadOption = loadOption;
        }

        public boolean loadEnv() {
            return loadEnv;
        }

        public boolean loadOption() {
            return loadOption;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Copied from org.apache.hadoop.hdfs.HdfsConfiguration to reduce dependency. */
    private static class HdfsConfiguration extends Configuration {

        private static void addDeprecatedKeys() {
            Configuration.addDeprecations(
                    new Configuration.DeprecationDelta[] {
                        new Configuration.DeprecationDelta(
                                "dfs.backup.address", "dfs.namenode.backup.address"),
                        new Configuration.DeprecationDelta(
                                "dfs.backup.http.address", "dfs.namenode.backup.http-address"),
                        new Configuration.DeprecationDelta(
                                "dfs.balance.bandwidthPerSec",
                                "dfs.datanode.balance.bandwidthPerSec"),
                        new Configuration.DeprecationDelta("dfs.data.dir", "dfs.datanode.data.dir"),
                        new Configuration.DeprecationDelta(
                                "dfs.http.address", "dfs.namenode.http-address"),
                        new Configuration.DeprecationDelta(
                                "dfs.https.address", "dfs.namenode.https-address"),
                        new Configuration.DeprecationDelta(
                                "dfs.max.objects", "dfs.namenode.max.objects"),
                        new Configuration.DeprecationDelta("dfs.name.dir", "dfs.namenode.name.dir"),
                        new Configuration.DeprecationDelta(
                                "dfs.name.dir.restore", "dfs.namenode.name.dir.restore"),
                        new Configuration.DeprecationDelta(
                                "dfs.name.edits.dir", "dfs.namenode.edits.dir"),
                        new Configuration.DeprecationDelta(
                                "dfs.read.prefetch.size", "dfs.client.read.prefetch.size"),
                        new Configuration.DeprecationDelta(
                                "dfs.safemode.extension", "dfs.namenode.safemode.extension"),
                        new Configuration.DeprecationDelta(
                                "dfs.safemode.threshold.pct",
                                "dfs.namenode.safemode.threshold-pct"),
                        new Configuration.DeprecationDelta(
                                "dfs.secondary.http.address",
                                "dfs.namenode.secondary.http-address"),
                        new Configuration.DeprecationDelta(
                                "dfs.socket.timeout", "dfs.client.socket-timeout"),
                        new Configuration.DeprecationDelta(
                                "fs.checkpoint.dir", "dfs.namenode.checkpoint.dir"),
                        new Configuration.DeprecationDelta(
                                "fs.checkpoint.edits.dir", "dfs.namenode.checkpoint.edits.dir"),
                        new Configuration.DeprecationDelta(
                                "fs.checkpoint.period", "dfs.namenode.checkpoint.period"),
                        new Configuration.DeprecationDelta(
                                "heartbeat.recheck.interval",
                                "dfs.namenode.heartbeat.recheck-interval"),
                        new Configuration.DeprecationDelta(
                                "dfs.https.client.keystore.resource",
                                "dfs.client.https.keystore.resource"),
                        new Configuration.DeprecationDelta(
                                "dfs.https.need.client.auth", "dfs.client.https.need-auth"),
                        new Configuration.DeprecationDelta(
                                "slave.host.name", "dfs.datanode.hostname"),
                        new Configuration.DeprecationDelta("session.id", "dfs.metrics.session-id"),
                        new Configuration.DeprecationDelta(
                                "dfs.access.time.precision", "dfs.namenode.accesstime.precision"),
                        new Configuration.DeprecationDelta(
                                "dfs.replication.considerLoad",
                                "dfs.namenode.replication.considerLoad"),
                        new Configuration.DeprecationDelta(
                                "dfs.replication.interval", "dfs.namenode.replication.interval"),
                        new Configuration.DeprecationDelta(
                                "dfs.replication.min", "dfs.namenode.replication.min"),
                        new Configuration.DeprecationDelta(
                                "dfs.replication.pending.timeout.sec",
                                "dfs.namenode.replication.pending.timeout-sec"),
                        new Configuration.DeprecationDelta(
                                "dfs.max-repl-streams", "dfs.namenode.replication.max-streams"),
                        new Configuration.DeprecationDelta(
                                "dfs.permissions", "dfs.permissions.enabled"),
                        new Configuration.DeprecationDelta(
                                "dfs.permissions.supergroup", "dfs.permissions.superusergroup"),
                        new Configuration.DeprecationDelta(
                                "dfs.write.packet.size", "dfs.client-write-packet-size"),
                        new Configuration.DeprecationDelta("dfs.block.size", "dfs.blocksize"),
                        new Configuration.DeprecationDelta(
                                "dfs.datanode.max.xcievers", "dfs.datanode.max.transfer.threads"),
                        new Configuration.DeprecationDelta(
                                "io.bytes.per.checksum", "dfs.bytes-per-checksum"),
                        new Configuration.DeprecationDelta(
                                "dfs.federation.nameservices", "dfs.nameservices"),
                        new Configuration.DeprecationDelta(
                                "dfs.federation.nameservice.id", "dfs.nameservice.id"),
                        new Configuration.DeprecationDelta(
                                "dfs.client.file-block-storage-locations.timeout",
                                "dfs.client.file-block-storage-locations.timeout.millis"),
                        new Configuration.DeprecationDelta(
                                "dfs.encryption.key.provider.uri",
                                "hadoop.security.key.provider.path")
                    });
        }

        static {
            addDeprecatedKeys();
            Configuration.addDefaultResource("hdfs-default.xml");
            Configuration.addDefaultResource("hdfs-site.xml");
        }
    }
}
