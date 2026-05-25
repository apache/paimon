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

package org.apache.paimon.cli;

import org.apache.paimon.options.Options;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/** Loads catalog configuration from a paimon.yaml file into Paimon {@link Options}. */
public class CliConfig {

    public static final String DEFAULT_CONFIG_FILE = "paimon.yaml";

    private CliConfig() {}

    public static Options load(String configPath) {
        Path path = Paths.get(configPath);
        if (!Files.exists(path)) {
            System.err.println("Configuration file not found: " + configPath);
            System.err.println(
                    "Please create a paimon.yaml file. Example:\n"
                            + "  metastore: filesystem\n"
                            + "  warehouse: /path/to/warehouse");
            System.exit(1);
        }

        try (InputStream in = new FileInputStream(path.toFile())) {
            Yaml yaml = new Yaml();
            Map<String, Object> yamlMap = yaml.load(in);
            if (yamlMap == null || yamlMap.isEmpty()) {
                System.err.println("Empty configuration file: " + configPath);
                System.exit(1);
            }
            return toOptions(yamlMap);
        } catch (FileNotFoundException e) {
            System.err.println("Configuration file not found: " + configPath);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Failed to read configuration file: " + e.getMessage());
            System.exit(1);
        }
        return new Options(); // unreachable
    }

    @SuppressWarnings("unchecked")
    private static Options toOptions(Map<String, Object> yamlMap) {
        Options options = new Options();
        flattenMap("", yamlMap, options);
        return options;
    }

    @SuppressWarnings("unchecked")
    private static void flattenMap(String prefix, Map<String, Object> map, Options options) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenMap(key, (Map<String, Object>) value, options);
            } else if (value != null) {
                options.set(key, String.valueOf(value));
            }
        }
    }
}
