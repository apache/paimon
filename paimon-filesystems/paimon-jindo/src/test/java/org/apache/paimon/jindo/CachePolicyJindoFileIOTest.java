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

package org.apache.paimon.jindo;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.HadoopOptionsProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.cache.CachePolicyFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ENABLED;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_ORIGIN_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_POLICY;
import static org.apache.paimon.rest.RESTCatalogOptions.IO_CACHE_WHITELIST_PATH;
import static org.assertj.core.api.Assertions.assertThat;

/** Verifies that the common policy wrapper preserves Jindo's native options. */
class CachePolicyJindoFileIOTest {

    private static final Path ROOT = new Path("oss://bucket/table");
    private static final Path DATA = new Path(ROOT, "bucket-0/data.orc");

    @Test
    void testRoutesNativeOptionsAndPreservesBackendConfiguration() throws Exception {
        try (FileIO fileIO = create(options())) {
            HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
            Options read = provider.hadoopOptions(DATA, "read");
            assertThat(read.get("fs.oss.endpoint")).isEqualTo("cache");
            assertThat(provider.hadoopOptions(DATA, "write").get("fs.oss.endpoint"))
                    .isEqualTo("origin");
            assertThat(
                            provider.hadoopOptions(new Path(ROOT, "snapshot/LATEST"), "read")
                                    .get("fs.oss.endpoint"))
                    .isEqualTo("origin");
            assertThat(read.get("fs.oss.accessKeyId")).isEqualTo("test-ak");
            assertThat(read.get("fs.oss.securityToken")).isEqualTo("test-token");
            assertThat(read.get("fs.oss.show-dir-timestamp")).isEqualTo("false");
            assertThat(read.get("fs.oss.user.agent.extended")).contains("tracking-info");
        }
    }

    @Test
    void testSerialization() throws Exception {
        try (FileIO fileIO = create(options());
                FileIO cloned = InstantiationUtil.clone(fileIO)) {
            assertThat(
                            ((HadoopOptionsProvider) cloned)
                                    .hadoopOptions(DATA, "read")
                                    .get("fs.oss.endpoint"))
                    .isEqualTo("cache");
        }
    }

    @Test
    void testDisabledPolicyReturnsSingleEndpointJindo() throws Exception {
        Options options = options();
        options.set(IO_CACHE_POLICY, "none");
        try (FileIO fileIO = create(options)) {
            assertThat(fileIO).isInstanceOf(JindoFileIO.class);
            assertThat(
                            ((HadoopOptionsProvider) fileIO)
                                    .hadoopOptions(DATA, "read")
                                    .get("fs.oss.endpoint"))
                    .isEqualTo("origin");
        }
    }

    @Test
    void testHadoopFallbackUsesRoutedEndpointWithoutChangingOriginalConfiguration()
            throws Exception {
        Options options = options();
        options.remove("fs.oss.accessKeyId");
        options.remove("fs.oss.accessKeySecret");
        options.remove("fs.oss.securityToken");
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("fs.oss.endpoint", "hadoop-configured");
        hadoopConf.set("fs.oss.securityToken", "hadoop-token");
        for (String policy : new String[] {"read", "none"}) {
            options.set(IO_CACHE_POLICY, policy);
            try (FileIO fileIO =
                    CachePolicyFileIO.create(
                            ROOT,
                            CatalogContext.create(
                                    options, hadoopConf, new TestingJindoLoader(), null))) {
                HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
                assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.endpoint"))
                        .isEqualTo("read".equals(policy) ? "cache" : "origin");
                assertThat(provider.hadoopOptions(DATA, "write").get("fs.oss.endpoint"))
                        .isEqualTo("origin");
                assertThat(provider.hadoopOptions(DATA, "read").get("fs.oss.securityToken"))
                        .isEqualTo("hadoop-token");
            }
            assertThat(hadoopConf.get("fs.oss.endpoint")).isEqualTo("hadoop-configured");
            assertThat(options.get("fs.oss.endpoint")).isEqualTo("cache");
        }
    }

    @Test
    void testDeprecatedJindoCacheKeepsItsOriginalSwitch() throws Exception {
        for (boolean legacyEnabled : new boolean[] {false, true}) {
            Options options = options();
            options.set(IO_CACHE_ENABLED, legacyEnabled);
            options.set(IO_CACHE_WHITELIST_PATH, "bucket-0");
            options.set("fs.jindocache.namespace.rpc.address", "legacy-cache:1234");
            try (FileIO fileIO = create(options)) {
                HadoopOptionsProvider provider = (HadoopOptionsProvider) fileIO;
                Options read = provider.hadoopOptions(DATA, "read");
                assertThat(read.get("fs.oss.endpoint")).isEqualTo("cache");
                assertThat(read.get("fs.xengine")).isEqualTo(legacyEnabled ? "jindocache" : null);
                assertThat(provider.hadoopOptions(DATA, "write").get("fs.xengine")).isNull();
                assertThat(
                                provider.hadoopOptions(new Path(ROOT, "snapshot/LATEST"), "read")
                                        .get("fs.xengine"))
                        .isNull();
            }
        }
    }

    private static FileIO create(Options options) throws Exception {
        return CachePolicyFileIO.create(
                ROOT, CatalogContext.create(options, new TestingJindoLoader(), null));
    }

    private static Options options() {
        Options options = new Options();
        options.set(IO_CACHE_POLICY, "read");
        options.set(IO_CACHE_ORIGIN_ENDPOINT, "origin");
        options.set("fs.oss.endpoint", "cache");
        options.set("fs.oss.accessKeyId", "test-ak");
        options.set("fs.oss.accessKeySecret", "test-sk");
        options.set("fs.oss.securityToken", "test-token");
        options.set("dlf.access-tracking.extended-info", "tracking-info");
        return options;
    }

    private static class TestingJindoLoader implements FileIOLoader {

        @Override
        public String getScheme() {
            return "oss";
        }

        @Override
        public FileIO load(Path path) {
            return new TestingJindoFileIO();
        }
    }

    private static class TestingJindoFileIO extends JindoFileIO {

        @Override
        public boolean exists(Path path) {
            return false;
        }
    }
}
