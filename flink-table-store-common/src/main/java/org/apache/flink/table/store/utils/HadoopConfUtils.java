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

package org.apache.flink.table.store.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Utility class for working with Hadoop-related classes. This should only be used if Hadoop is on
 * the classpath.
 */
public class HadoopConfUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopConfUtils.class);
    public static final String HADOOP_HOME_ENV = "HADOOP_HOME";
    public static final String HADOOP_CONF_ENV = "HADOOP_CONF_DIR";

    public static Configuration getHadoopConfiguration() {

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

        final String hadoopHomeDir = System.getenv(HADOOP_HOME_ENV);
        if (hadoopHomeDir != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_HOME: {}", hadoopHomeDir);
            possibleHadoopConfPaths[0] = hadoopHomeDir + "/conf";
            possibleHadoopConfPaths[1] = hadoopHomeDir + "/etc/hadoop"; // hadoop 2.2
        }

        for (String possibleHadoopConfPath : possibleHadoopConfPaths) {
            if (possibleHadoopConfPath != null) {
                foundHadoopConfiguration = addHadoopConfIfFound(result, possibleHadoopConfPath);
            }
        }

        // Approach 2: HADOOP_CONF_DIR environment variable
        String hadoopConfDir = System.getenv(HADOOP_CONF_ENV);
        if (hadoopConfDir != null) {
            LOG.debug("Searching Hadoop configuration files in HADOOP_CONF_DIR: {}", hadoopConfDir);
            foundHadoopConfiguration =
                    addHadoopConfIfFound(result, hadoopConfDir) || foundHadoopConfiguration;
        }

        if (!foundHadoopConfiguration) {
            LOG.warn("Could not find Hadoop configuration via any of the supported methods ");
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
}
