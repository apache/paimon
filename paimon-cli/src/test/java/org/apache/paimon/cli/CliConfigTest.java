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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class CliConfigTest {

    @TempDir Path tempDir;

    @Test
    void testLoadYaml() throws Exception {
        Path configFile = tempDir.resolve("paimon.yaml");
        String yaml = "metastore: filesystem\nwarehouse: /tmp/warehouse\n";
        Files.write(configFile, yaml.getBytes(StandardCharsets.UTF_8));

        Options options = CliConfig.load(configFile.toString());
        assertThat(options.get("metastore")).isEqualTo("filesystem");
        assertThat(options.get("warehouse")).isEqualTo("/tmp/warehouse");
    }

    @Test
    void testLoadYamlWithNestedKeys() throws Exception {
        Path configFile = tempDir.resolve("paimon.yaml");
        String yaml =
                "metastore: rest\n" + "uri: http://localhost:8080\n" + "warehouse: my_catalog\n";
        Files.write(configFile, yaml.getBytes(StandardCharsets.UTF_8));

        Options options = CliConfig.load(configFile.toString());
        assertThat(options.get("metastore")).isEqualTo("rest");
        assertThat(options.get("uri")).isEqualTo("http://localhost:8080");
        assertThat(options.get("warehouse")).isEqualTo("my_catalog");
    }

    @Test
    void testFlattenedNestedMap() throws Exception {
        Path configFile = tempDir.resolve("paimon.yaml");
        String yaml =
                "metastore: filesystem\nwarehouse: /tmp/wh\nfs:\n  oss:\n    endpoint: oss-cn-hangzhou.aliyuncs.com\n";
        Files.write(configFile, yaml.getBytes(StandardCharsets.UTF_8));

        Options options = CliConfig.load(configFile.toString());
        assertThat(options.get("fs.oss.endpoint")).isEqualTo("oss-cn-hangzhou.aliyuncs.com");
    }
}
