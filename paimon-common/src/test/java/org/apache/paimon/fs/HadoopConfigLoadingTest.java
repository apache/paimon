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

package org.apache.paimon.fs;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.CommonTestUtils;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that validate the loading of the Hadoop configuration, relative to entries in the Paimon
 * configuration and the environment variables.
 */
@SuppressWarnings("deprecation")
public class HadoopConfigLoadingTest {

    private static final String IN_CP_CONFIG_KEY = "cp_conf_key";
    private static final String IN_CP_CONFIG_VALUE = "oompf!";

    private @TempDir Path tempDir;

    @Test
    public void loadFromClasspathByDefault() {
        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(new Options());

        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    public void loadFromHadoopConfEntry() throws Exception {
        final String k1 = "singing?";
        final String v1 = "rain!";

        final String k2 = "dancing?";
        final String v2 = "shower!";

        final File confDir = tempDir.toFile();

        final File file1 = new File(confDir, "core-site.xml");
        final File file2 = new File(confDir, "hdfs-site.xml");

        printConfig(file1, k1, v1);
        printConfig(file2, k2, v2);

        final Options options = new Options();
        options.setString(HadoopUtils.PATH_HADOOP_CONFIG, confDir.getAbsolutePath());

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(options);

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    public void loadFromEnvVariables() throws Exception {
        final String k1 = "where?";
        final String v1 = "I'm on a boat";
        final String k2 = "when?";
        final String v2 = "midnight";
        final String k3 = "why?";
        final String v3 = "what do you think?";
        final String k4 = "which way?";
        final String v4 = "south, always south...";
        final String k5 = "how long?";
        final String v5 = "an eternity";
        final String k6 = "for real?";
        final String v6 = "quite so...";

        final File hadoopConfDir = tempDir.resolve("hadoop_conf").toFile();

        final File hadoopHome = tempDir.resolve("hadoop_home").toFile();

        final File hadoopHomeConf = new File(hadoopHome, "conf");
        final File hadoopHomeEtc = new File(hadoopHome, "etc/hadoop");

        assertThat(hadoopConfDir.mkdirs()).isTrue();
        assertThat(hadoopHomeConf.mkdirs()).isTrue();
        assertThat(hadoopHomeEtc.mkdirs()).isTrue();

        final File file1 = new File(hadoopConfDir, "core-site.xml");
        final File file2 = new File(hadoopConfDir, "hdfs-site.xml");
        final File file3 = new File(hadoopHomeConf, "core-site.xml");
        final File file4 = new File(hadoopHomeConf, "hdfs-site.xml");
        final File file5 = new File(hadoopHomeEtc, "core-site.xml");
        final File file6 = new File(hadoopHomeEtc, "hdfs-site.xml");

        printConfig(file1, k1, v1);
        printConfig(file2, k2, v2);
        printConfig(file3, k3, v3);
        printConfig(file4, k4, v4);
        printConfig(file5, k5, v5);
        printConfig(file6, k6, v6);

        final Configuration hadoopConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put(HadoopUtils.HADOOP_CONF_ENV, hadoopConfDir.getAbsolutePath());
        newEnv.put(HadoopUtils.HADOOP_HOME_ENV, hadoopHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hadoopConf = HadoopUtils.getHadoopConfiguration(new Options());
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);
        assertThat(hadoopConf.get(k3, null)).isEqualTo(v3);
        assertThat(hadoopConf.get(k4, null)).isEqualTo(v4);
        assertThat(hadoopConf.get(k5, null)).isEqualTo(v5);
        assertThat(hadoopConf.get(k6, null)).isEqualTo(v6);

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    public void loadOverlappingConfig() throws Exception {
        final String k1 = "key";

        final String v1 = "from HADOOP_HOME/etc/hadoop or HADOOP_HOME/conf";
        final String v2 = "from HADOOP_CONF_DIR";
        final String v3 = "from Paimon config hadoop-conf-dir";
        final String v4 = "from Paimon advanced configuration";

        final File hadoopHome = tempDir.resolve("hadoopHome").toFile();
        final File hadoopHomeConf = tempDir.resolve("hadoopHome/conf").toFile();
        final File hadoopHomeEtc = tempDir.resolve("hadoopHome/etc/hadoop").toFile();
        final File hadoopConfDir = tempDir.resolve("hadoopConfDir").toFile();
        final File hadoopPaimonConfDir = tempDir.resolve("hadoopPaimonConfDir").toFile();

        assertThat(hadoopHomeConf.mkdirs()).isTrue();
        assertThat(hadoopHomeEtc.mkdirs()).isTrue();
        assertThat(hadoopConfDir.mkdirs()).isTrue();
        assertThat(hadoopPaimonConfDir.mkdirs()).isTrue();

        final File fileHomeEtc = new File(hadoopHomeEtc, "core-site.xml");
        final File fileHomeConf = new File(hadoopHomeConf, "core-site.xml");
        final File fileConfDir = new File(hadoopConfDir, "core-site.xml");
        final File filePaimonConfDir = new File(hadoopPaimonConfDir, "core-site.xml");

        Map<String, String> properties1 = new HashMap<>();
        properties1.put(k1, v1);
        printConfigs(fileHomeEtc, properties1);
        printConfigs(fileHomeConf, properties1);

        Map<String, String> properties2 = new HashMap<>();
        properties2.put(k1, v2);
        printConfigs(fileConfDir, properties2);

        Map<String, String> properties3 = new HashMap<>();
        properties3.put(k1, v3);
        printConfigs(filePaimonConfDir, properties3);

        final Options options = new Options();

        final Configuration hadoopConf1;
        final Configuration hadoopConf2;
        final Configuration hadoopConf3;
        final Configuration hadoopConf4;
        final Configuration hadoopConf5;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put(HadoopUtils.HADOOP_HOME_ENV, hadoopHome.getAbsolutePath());
        final Map<String, String> newEnv1 = new HashMap<>(newEnv);
        newEnv1.put(HadoopUtils.HADOOP_CONF_ENV, hadoopConfDir.getAbsolutePath());
        try {
            options.set(HadoopUtils.HADOOP_CONF_LOADER, HadoopUtils.HadoopConfigLoader.ENV);
            CommonTestUtils.setEnv(newEnv);
            hadoopConf1 = HadoopUtils.getHadoopConfiguration(options);

            CommonTestUtils.setEnv(newEnv1);
            hadoopConf2 = HadoopUtils.getHadoopConfiguration(options);

            options.set(HadoopUtils.HADOOP_CONF_LOADER, HadoopUtils.HadoopConfigLoader.OPTION);
            options.setString(
                    HadoopUtils.PATH_HADOOP_CONFIG, hadoopPaimonConfDir.getAbsolutePath());
            hadoopConf3 = HadoopUtils.getHadoopConfiguration(options);

            options.set(HadoopUtils.HADOOP_CONF_LOADER, HadoopUtils.HadoopConfigLoader.ALL);
            hadoopConf4 = HadoopUtils.getHadoopConfiguration(options);

            options.set("hadoop." + k1, v4);
            hadoopConf5 = HadoopUtils.getHadoopConfiguration(options);
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        assertThat(hadoopConf1.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf1.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);

        assertThat(hadoopConf2.get(k1, null)).isEqualTo(v2);
        assertThat(hadoopConf2.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);

        assertThat(hadoopConf3.get(k1, null)).isEqualTo(v3);
        assertThat(hadoopConf3.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);

        assertThat(hadoopConf4.get(k1, null)).isEqualTo(v3);
        assertThat(hadoopConf4.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);

        assertThat(hadoopConf5.get(k1, null)).isEqualTo(v4);
        assertThat(hadoopConf5.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    @Test
    public void loadFromPaimonConfEntry() {
        final String prefix = "hadoop.";

        final String k1 = "brooklyn";
        final String v1 = "nets";

        final String k2 = "miami";
        final String v2 = "heat";

        final String k3 = "philadelphia";
        final String v3 = "76ers";

        final String k4 = "golden.state";
        final String v4 = "warriors";

        final String k5 = "oklahoma.city";
        final String v5 = "thunders";

        final Options options = new Options();
        options.setString(prefix + k1, v1);
        options.setString(prefix + k2, v2);
        options.setString(prefix + k3, v3);
        options.setString(prefix + k4, v4);
        options.setString(k5, v5);

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(options);

        // contains extra entries
        assertThat(hadoopConf.get(k1, null)).isEqualTo(v1);
        assertThat(hadoopConf.get(k2, null)).isEqualTo(v2);
        assertThat(hadoopConf.get(k3, null)).isEqualTo(v3);
        assertThat(hadoopConf.get(k4, null)).isEqualTo(v4);
        assertThat(hadoopConf.get(k5)).isNull();

        // also contains classpath defaults
        assertThat(hadoopConf.get(IN_CP_CONFIG_KEY, null)).isEqualTo(IN_CP_CONFIG_VALUE);
    }

    private static void printConfig(File file, String key, String value) throws IOException {
        Map<String, String> map = new HashMap<>(1);
        map.put(key, value);
        printConfigs(file, map);
    }

    private static void printConfigs(File file, Map<String, String> properties) throws IOException {
        try (PrintStream out = new PrintStream(Files.newOutputStream(file.toPath()))) {
            out.println("<?xml version=\"1.0\"?>");
            out.println("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>");
            out.println("<configuration>");
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                out.println("\t<property>");
                out.println("\t\t<name>" + entry.getKey() + "</name>");
                out.println("\t\t<value>" + entry.getValue() + "</value>");
                out.println("\t</property>");
            }
            out.println("</configuration>");
        }
    }
}
