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
        Configuration result = new Configuration();
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

    static {
        // load the hdfs-site.xml and hdfs-default.xml from the classpath
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
    }
}
