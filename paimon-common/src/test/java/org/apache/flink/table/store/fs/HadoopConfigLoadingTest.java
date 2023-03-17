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

package org.apache.flink.table.store.fs;

import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.utils.CommonTestUtils;
import org.apache.flink.table.store.utils.HadoopUtils;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that validate the loading of the Hadoop configuration, relative to entries in the Flink
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

        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
    }

    @Test
    public void loadFromLegacyConfigEntries() throws Exception {
        final String k1 = "k1 prop";
        final String v1 = "value1";

        final String k2 = "k2 prop";
        final String v2 = "value2";

        final File file1 = tempDir.resolve("core-site.xml").toFile();
        final File file2 = tempDir.resolve("hdfs-site.xml").toFile();

        printConfig(file1, k1, v1);
        printConfig(file2, k2, v2);

        final Options options = new Options();
        options.set(HadoopUtils.HDFS_DEFAULT_CONFIG, file1.getAbsolutePath());
        options.set(HadoopUtils.HDFS_SITE_CONFIG, file2.getAbsolutePath());

        Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(options);

        // contains extra entries
        assertEquals(v1, hadoopConf.get(k1, null));
        assertEquals(v2, hadoopConf.get(k2, null));

        // also contains classpath defaults
        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
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
        assertEquals(v1, hadoopConf.get(k1, null));
        assertEquals(v2, hadoopConf.get(k2, null));

        // also contains classpath defaults
        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
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

        assertTrue(hadoopConfDir.mkdirs());
        assertTrue(hadoopHomeConf.mkdirs());
        assertTrue(hadoopHomeEtc.mkdirs());

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
        assertEquals(v1, hadoopConf.get(k1, null));
        assertEquals(v2, hadoopConf.get(k2, null));
        assertEquals(v3, hadoopConf.get(k3, null));
        assertEquals(v4, hadoopConf.get(k4, null));
        assertEquals(v5, hadoopConf.get(k5, null));
        assertEquals(v6, hadoopConf.get(k6, null));

        // also contains classpath defaults
        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
    }

    @Test
    public void loadOverlappingConfig() throws Exception {
        final String k1 = "key1";
        final String k2 = "key2";
        final String k3 = "key3";
        final String k4 = "key4";
        final String k5 = "key5";

        final String v1 = "from HADOOP_CONF_DIR";
        final String v2 = "from Flink config `fs.hdfs.hadoopconf`";
        final String v3 = "from Flink config `fs.hdfs.hdfsdefault`";
        final String v4 = "from HADOOP_HOME/etc/hadoop";
        final String v5 = "from HADOOP_HOME/conf";

        final File hadoopConfDir = tempDir.resolve("hadoopConfDir").toFile();
        final File hadoopConfEntryDir = tempDir.resolve("hadoopConfEntryDir").toFile();
        final File legacyConfDir = tempDir.resolve("legacyConfDir").toFile();
        final File hadoopHome = tempDir.resolve("hadoopHome").toFile();

        final File hadoopHomeConf = tempDir.resolve("hadoopHome/conf").toFile();
        final File hadoopHomeEtc = tempDir.resolve("hadoopHome/etc/hadoop").toFile();

        assertTrue(hadoopConfDir.mkdirs());
        assertTrue(hadoopConfEntryDir.mkdirs());
        assertTrue(legacyConfDir.mkdirs());
        assertTrue(hadoopHomeConf.mkdirs());
        assertTrue(hadoopHomeEtc.mkdirs());

        final File file1 = new File(hadoopConfDir, "core-site.xml");
        final File file2 = new File(hadoopConfEntryDir, "core-site.xml");
        final File file3 = new File(legacyConfDir, "core-site.xml");
        final File file4 = new File(hadoopHomeEtc, "core-site.xml");
        final File file5 = new File(hadoopHomeConf, "core-site.xml");

        printConfig(file1, k1, v1);

        Map<String, String> properties2 = new HashMap<>();
        properties2.put(k1, v2);
        properties2.put(k2, v2);
        printConfigs(file2, properties2);

        Map<String, String> properties3 = new HashMap<>();
        properties3.put(k1, v3);
        properties3.put(k2, v3);
        properties3.put(k3, v3);
        printConfigs(file3, properties3);

        Map<String, String> properties4 = new HashMap<>();
        properties4.put(k1, v4);
        properties4.put(k2, v4);
        properties4.put(k3, v4);
        properties4.put(k4, v4);
        printConfigs(file4, properties4);

        Map<String, String> properties5 = new HashMap<>();
        properties5.put(k1, v5);
        properties5.put(k2, v5);
        properties5.put(k3, v5);
        properties5.put(k4, v5);
        properties5.put(k5, v5);
        printConfigs(file5, properties5);

        final Options options = new Options();
        options.setString(HadoopUtils.PATH_HADOOP_CONFIG, hadoopConfEntryDir.getAbsolutePath());
        options.setString(HadoopUtils.HDFS_DEFAULT_CONFIG, file3.getAbsolutePath());

        final Configuration hadoopConf;

        final Map<String, String> originalEnv = System.getenv();
        final Map<String, String> newEnv = new HashMap<>(originalEnv);
        newEnv.put(HadoopUtils.HADOOP_CONF_ENV, hadoopConfDir.getAbsolutePath());
        newEnv.put(HadoopUtils.HADOOP_HOME_ENV, hadoopHome.getAbsolutePath());
        try {
            CommonTestUtils.setEnv(newEnv);
            hadoopConf = HadoopUtils.getHadoopConfiguration(options);
        } finally {
            CommonTestUtils.setEnv(originalEnv);
        }

        // contains extra entries
        assertEquals(v1, hadoopConf.get(k1, null));
        assertEquals(v2, hadoopConf.get(k2, null));
        assertEquals(v3, hadoopConf.get(k3, null));
        assertEquals(v4, hadoopConf.get(k4, null));
        assertEquals(v5, hadoopConf.get(k5, null));

        // also contains classpath defaults
        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
    }

    @Test
    public void loadFromFlinkConfEntry() throws Exception {
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
        assertEquals(v1, hadoopConf.get(k1, null));
        assertEquals(v2, hadoopConf.get(k2, null));
        assertEquals(v3, hadoopConf.get(k3, null));
        assertEquals(v4, hadoopConf.get(k4, null));
        assertNull(hadoopConf.get(k5));

        // also contains classpath defaults
        assertEquals(IN_CP_CONFIG_VALUE, hadoopConf.get(IN_CP_CONFIG_KEY, null));
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
