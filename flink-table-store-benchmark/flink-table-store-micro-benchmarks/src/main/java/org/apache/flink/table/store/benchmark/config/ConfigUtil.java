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

package org.apache.flink.table.store.benchmark.config;

import org.apache.flink.table.store.options.Options;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

/**
 * Config utils to load benchmark config from yaml in classpath, the main class is copied from
 * {@code ConfigUtil} in flink-benchmarks.
 */
public class ConfigUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    private static final String BENCHMARK_CONF = "benchmark-conf.yaml";

    /** Load benchmark conf from classpath. */
    public static Options loadBenchMarkConf() {
        InputStream inputStream =
                ConfigUtil.class.getClassLoader().getResourceAsStream(BENCHMARK_CONF);
        return loadYAMLResource(inputStream);
    }

    /**
     * This is copied from {@code GlobalConfiguration#loadYAMLResource} to avoid depending
     * on @Internal api.
     */
    private static Options loadYAMLResource(InputStream inputStream) {
        final Options config = new Options();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    LOG.info("Loading configuration property: {}, {}", key, value);
                    config.setString(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        return config;
    }

    /**
     * Create file data dir from given configuration.
     *
     * @param configuration the configuration
     * @return the file data dir
     */
    public static String createFileDataDir(Options configuration) {
        return configuration.get(FileBenchmarkOptions.FILE_DATA_BASE_DIR)
                + "/"
                + UUID.randomUUID().toString();
    }
}
