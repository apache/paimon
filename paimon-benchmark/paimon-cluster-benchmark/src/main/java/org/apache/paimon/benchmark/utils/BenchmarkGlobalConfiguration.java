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

package org.apache.paimon.benchmark.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Global configuration object for benchmark. Similar to Java properties configuration objects it
 * includes key-value pairs which represent the framework's configuration.
 */
public final class BenchmarkGlobalConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(org.apache.flink.configuration.GlobalConfiguration.class);

    public static final String BENCHMARK_CONF_FILENAME = "benchmark.yaml";
    private static final String BENCHMARK_CONF_DIR_ENV_NAME = "BENCHMARK_CONF_DIR";

    // --------------------------------------------------------------------------------------------

    private BenchmarkGlobalConfiguration() {}

    // --------------------------------------------------------------------------------------------

    /**
     * Loads the global configuration from the environment. Fails if an error occurs during loading.
     * Returns an empty configuration object if the environment variable is not set. In production
     * this variable is set but tests and local execution/debugging don't have this environment
     * variable set. That's why we should fail if it is not set.
     *
     * @return Returns the Configuration
     */
    public static Configuration loadConfiguration() {
        return loadConfiguration(new Configuration());
    }

    /**
     * Loads the global configuration and adds the given dynamic properties configuration.
     *
     * @param dynamicProperties The given dynamic properties
     * @return Returns the loaded global configuration with dynamic properties
     */
    public static Configuration loadConfiguration(Configuration dynamicProperties) {
        final String configDir = System.getenv(BENCHMARK_CONF_DIR_ENV_NAME);
        if (configDir == null) {
            return new Configuration(dynamicProperties);
        }

        return loadConfiguration(configDir, dynamicProperties);
    }

    /**
     * Loads the configuration files from the specified directory.
     *
     * <p>YAML files are supported as configuration files.
     *
     * @param configDir the directory which contains the configuration files
     */
    public static Configuration loadConfiguration(final String configDir) {
        return loadConfiguration(configDir, null);
    }

    /**
     * Loads the configuration files from the specified directory. If the dynamic properties
     * configuration is not null, then it is added to the loaded configuration.
     *
     * @param configDir directory to load the configuration from
     * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
     * @return The configuration loaded from the given configuration directory
     */
    public static Configuration loadConfiguration(
            final String configDir, @Nullable final Configuration dynamicProperties) {

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '"
                            + configDir
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not describe an existing directory.");
        }

        // get benchmark yaml configuration file
        final File yamlConfigFile = new File(confDirFile, BENCHMARK_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The benchmark config file '"
                            + yamlConfigFile
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not exist.");
        }

        Configuration configuration = new Configuration();
        try (InputStream inputStream = new FileInputStream(yamlConfigFile)) {
            Map<?, ?> yaml = BenchmarkUtils.YAML_MAPPER.readValue(inputStream, Map.class);
            for (Map.Entry<?, ?> entry : yaml.entrySet()) {
                configuration.setString(entry.getKey().toString(), entry.getValue().toString());
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        if (dynamicProperties != null) {
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }
}
