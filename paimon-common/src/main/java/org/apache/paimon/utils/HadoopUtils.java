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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;

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

    public static final ConfigOption<Boolean> HADOOP_LOAD_DEFAULT_CONFIG =
            key("hadoop-load-default-config")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Specifies whether load the default configuration from core-default.xml、hdfs-default.xml, which may lead larger size for the serialization of table.");

    private static final String[] CONFIG_PREFIXES = {"hadoop."};
    public static final String HADOOP_HOME_ENV = "HADOOP_HOME";
    public static final String HADOOP_CONF_ENV = "HADOOP_CONF_DIR";

    /** Path to Hadoop configuration. */
    public static final String PATH_HADOOP_CONFIG = "hadoop-conf-dir";

    public static Configuration getHadoopConfiguration(Options options) {

        // Instantiate an HdfsConfiguration to load the hdfs-site.xml and hdfs-default.xml
        // from the classpath

        Boolean loadDefaultConfig = options.get(HADOOP_LOAD_DEFAULT_CONFIG);
        if (loadDefaultConfig) {
            LOG.debug("Load the default value for configuration.");
        }
        Configuration result = new HdfsConfiguration(loadDefaultConfig);
        boolean foundHadoopConfiguration = false;

        // We need to load both core-site.xml and hdfs-site.xml to determine the default fs path and
        // the hdfs configuration.
        // The properties of a newly added resource will override the ones in previous resources, so
        // a configuration
        // file with higher priority should be added later.

        HadoopConfigLoader loader = options.get(HADOOP_CONF_LOADER);

        // The HDFS configuration priority from low to high is as follows:
        // 1. HADOOP_HOME
        // 2. HADOOP_CONF_DIR
        // 3. Paimon catalog or paimon table configuration(hadoop-conf-dir)
        // 4. paimon table advanced configurations

        // Approach 1: HADOOP_HOME environment variables
        String[] possibleHadoopConfPaths = new String[2];
        final String hadoopHomeDir = System.getenv(HADOOP_HOME_ENV);
        if (hadoopHomeDir != null && loader.loadEnv()) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_HOME: {}", hadoopHomeDir);
            possibleHadoopConfPaths[0] = hadoopHomeDir + "/conf";
            possibleHadoopConfPaths[1] = hadoopHomeDir + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (!StringUtils.isNullOrWhitespaceOnly(possibleHadoopConfPath)) {
                foundHadoopConfiguration =
                        addHadoopConfIfFound(result, possibleHadoopConfPath, options);
            }
        }

        // Approach 2: HADOOP_CONF_DIR environment variable
        String hadoopConfDir = System.getenv(HADOOP_CONF_ENV);
        if (!StringUtils.isNullOrWhitespaceOnly(hadoopConfDir) && loader.loadEnv()) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_CONF_DIR: {}", hadoopConfDir);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfDir, options)
                            || foundHadoopConfiguration;
        }

        // Approach 3: Paimon table or paimon catalog hadoop conf
        hadoopConfDir = options.getString(PATH_HADOOP_CONFIG, null);
        if (!StringUtils.isNullOrWhitespaceOnly(hadoopConfDir) && loader.loadOption()) {
            LOG.debug("Searching Hadoop configuration files in Paimon config: {}", hadoopConfDir);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfDir, options)
                            || foundHadoopConfiguration;
        }

        // Approach 4: Paimon advanced configuration
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
    public static boolean addHadoopConfIfFound(
            Configuration configuration, String possibleHadoopConfPath, Options options) {
        Path root = new Path(possibleHadoopConfPath);

        try {
            FileIO fileIO = FileIO.get(root, CatalogContext.create(options, configuration));
            boolean foundHadoopConfiguration = false;

            if (fileIO.exists(root)) {
                Path path = new Path(possibleHadoopConfPath, "core-site.xml");
                if (fileIO.exists(path)) {
                    readHadoopXml(fileIO.readFileUtf8(path), configuration);
                    LOG.debug(
                            "Adding "
                                    + possibleHadoopConfPath
                                    + "/core-site.xml to hadoop configuration");
                    foundHadoopConfiguration = true;
                }

                path = new Path(possibleHadoopConfPath, "hdfs-site.xml");
                if (fileIO.exists(path)) {
                    readHadoopXml(fileIO.readFileUtf8(path), configuration);
                    LOG.debug(
                            "Adding "
                                    + possibleHadoopConfPath
                                    + "/hdfs-site.xml to hadoop configuration");
                    foundHadoopConfiguration = true;
                }

                // Add mapred-site.xml. We need to read configurations like compression codec.
                path = new Path(possibleHadoopConfPath, "mapred-site.xml");
                if (fileIO.exists(path)) {
                    readHadoopXml(fileIO.readFileUtf8(path), configuration);
                    LOG.debug(
                            "Adding "
                                    + possibleHadoopConfPath
                                    + "/mapred-site.xml to hadoop configuration");
                    foundHadoopConfiguration = true;
                }
            }

            return foundHadoopConfiguration;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void readHadoopXml(String xml, Configuration conf) {
        NodeList propertyNodes;
        try {
            propertyNodes =
                    DocumentBuilderFactory.newInstance()
                            .newDocumentBuilder()
                            .parse(new InputSource(new StringReader(xml)))
                            .getElementsByTagName("property");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == 1) {
                Element propertyElement = (Element) propertyNode;
                String key = propertyElement.getElementsByTagName("name").item(0).getTextContent();
                String value =
                        propertyElement.getElementsByTagName("value").item(0).getTextContent();
                if (!StringUtils.isNullOrWhitespaceOnly(value)) {
                    conf.set(key, value);
                }
            }
        }
    }

    /** Specifies the way of loading hadoop config. */
    public enum HadoopConfigLoader implements DescribedEnum {
        ALL("all", "Load Hadoop conf from environment variables and catalog option.", true, true),
        ENV("env", "Load Hadoop conf from environment variables only.", true, false),
        OPTION("option", "Load Hadoop conf from catalog or table option only.", false, true);

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
}
