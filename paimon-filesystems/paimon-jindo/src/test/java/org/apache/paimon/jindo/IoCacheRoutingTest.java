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
import org.apache.paimon.fs.Path;
import org.apache.paimon.jindo.IoCacheRouting.CacheTarget;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.FileType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IoCacheRouting}, including the routing cases shared by all clients. */
public class IoCacheRoutingTest {

    private static final String TABLE_ROOT = "oss://bkt/db1.db/t1";
    private static final Path DATA_FILE =
            new Path(
                    TABLE_ROOT
                            + "/dt=2026-09-30/bucket-0/data-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b-0.orc");
    private static final Path MANIFEST =
            new Path(TABLE_ROOT + "/manifest/manifest-8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b-0");

    @ParameterizedTest(name = "{0}")
    @MethodSource("endpointCases")
    public void testEndpointOfEachRequest(
            String name, Options options, String op, String path, String expect) {
        // RESTTokenFileIO puts a client-side dlf.oss-endpoint into fs.oss.endpoint
        String override = options.get("dlf.oss-endpoint");
        if (override != null) {
            options.set("fs.oss.endpoint", override);
        }
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));
        Options hadoopOptions = fileIO.hadoopOptions(new Path(path), opType(op));
        assertThat(host(hadoopOptions.get("fs.oss.endpoint"))).isEqualTo(host(expect));
    }

    @ParameterizedTest(name = "{0} ({3})")
    @MethodSource("fileTypeCases")
    public void testCacheableType(String path, Options options, String type, String reason) {
        assertThat(
                        IoCacheRouting.cacheableType(
                                new Path(path), IoCacheRouting.dataFilePrefixes(options)))
                .isEqualTo(type == null ? null : fileType(type));
    }

    @Test
    public void testTargetNames() {
        Options options = singleTargetOptions();
        options.set("io-cache.targets", " Accel , cluster-1,accel,1x,a_b,,-x");
        options.set("io-cache.target.accel.endpoint", "http://a");
        options.set("io-cache.target.cluster-1.endpoint", "http://c");
        assertThat(IoCacheRouting.create(options).targets())
                .extracting(target -> target.name)
                .containsExactly("accel", "cluster-1");

        Options invalidNames = singleTargetOptions();
        invalidNames.set("io-cache.targets", "Bad_Name");
        assertThat(IoCacheRouting.create(invalidNames)).isNull();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("policyCases")
    public void testCachePolicyUsesCompleteCaseInsensitiveTokens(
            String policy, boolean readCache, boolean metaCache) {
        Options options = singleTargetOptions();
        options.set("io-cache.policy", policy);
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));
        assertThat(host(fileIO.hadoopOptions(DATA_FILE, "read").get("fs.oss.endpoint")))
                .isEqualTo(readCache ? "cache:8080" : "oss-cn-hangzhou.aliyuncs.com");
        assertThat(host(fileIO.hadoopOptions(DATA_FILE, "meta").get("fs.oss.endpoint")))
                .isEqualTo(
                        metaCache
                                ? "cache:8080"
                                : readCache
                                        ? "origin.example.com"
                                        : "oss-cn-hangzhou.aliyuncs.com");
    }

    private static List<Arguments> policyCases() {
        return java.util.Arrays.asList(
                Arguments.of(" READ , Meta ", true, true),
                Arguments.of("read,NONE", false, false),
                Arguments.of("thread,metadata", false, false),
                Arguments.of("read,nonetheless", true, false));
    }

    @Test
    public void testRoutedName() {
        String routes = " meta = Accel ; data,BUCKET-INDEX=cluster;=x;junk;file-index=;*=all;x=y";
        assertThat(IoCacheRouting.routedName(routes, FileType.META)).isEqualTo("accel");
        assertThat(IoCacheRouting.routedName(routes, FileType.DATA)).isEqualTo("cluster");
        assertThat(IoCacheRouting.routedName(routes, FileType.BUCKET_INDEX)).isEqualTo("cluster");
        assertThat(IoCacheRouting.routedName(routes, FileType.FILE_INDEX)).isEqualTo("all");
        assertThat(IoCacheRouting.routedName("meta=accel", FileType.DATA)).isNull();
    }

    @Test
    public void testTargetSettings() {
        Options options = twoTargetOptions();
        options.set("io-cache.target.accel.region", "cn-hangzhou");
        options.set("io-cache.target.cluster.path-style-access", " TRUE ");
        options.set("io-cache.target.off.endpoint", "");
        options.set("io-cache.targets", "accel,off,cluster,missing");
        IoCacheRouting routing = IoCacheRouting.create(options);

        assertThat(routing.targets())
                .containsExactly(
                        new CacheTarget("accel", "https://accel.example.com", false, "cn-hangzhou"),
                        new CacheTarget("cluster", "http://10.0.0.1:8080", true, null));
        assertThat(routing.targetOf(DATA_FILE).name).isEqualTo("cluster");
        assertThat(routing.targetOf(MANIFEST).name).isEqualTo("accel");
        assertThat(routing.targetOf(new Path(TABLE_ROOT + "/snapshot/snapshot-1"))).isNull();

        Options bare = singleTargetOptions();
        bare.set("io-cache.endpoint", "cache.example.com");
        assertThat(IoCacheRouting.create(bare).targetOf(DATA_FILE).url)
                .isEqualTo("https://cache.example.com");
    }

    @Test
    public void testNoIoCacheRouting() {
        Options override = singleTargetOptions();
        override.set("dlf.oss-endpoint", "oss-cn-hangzhou.aliyuncs.com");
        assertThat(IoCacheRouting.create(override)).isNull();

        Options disabled = singleTargetOptions();
        disabled.set("io-cache.enabled", "false");
        assertThat(IoCacheRouting.create(disabled)).isNull();

        Options writeOnly = singleTargetOptions();
        writeOnly.set("io-cache.policy", "write");
        IoCacheRouting writes = IoCacheRouting.create(writeOnly);
        assertThat(writes.writeCacheEnabled()).isTrue();
        assertThat(writes.readCacheEnabled() || writes.metaCacheEnabled()).isFalse();

        Options none = singleTargetOptions();
        none.remove("io-cache.endpoint");
        assertThat(IoCacheRouting.create(none)).isNull();

        // A declared target without an endpoint sends every request to origin.
        Options empty = singleTargetOptions();
        empty.set("io-cache.endpoint", "");
        IoCacheRouting routing = IoCacheRouting.create(empty);
        assertThat(routing.targets()).isEmpty();
        assertThat(routing.targetOf(DATA_FILE)).isNull();
        assertThat(routing.ossEndpoint()).isEqualTo("https://origin.example.com");
    }

    @Test
    public void testOssEndpoint() {
        Options options = singleTargetOptions();
        assertThat(IoCacheRouting.create(options).ossEndpoint())
                .isEqualTo("https://origin.example.com");
        options.remove("io-cache.origin.endpoint");
        assertThat(IoCacheRouting.create(options).ossEndpoint())
                .isEqualTo("oss-cn-hangzhou.aliyuncs.com");
    }

    @Test
    public void testSerializable() throws Exception {
        IoCacheRouting routing =
                InstantiationUtil.clone(
                        IoCacheRouting.create(twoTargetOptions()), getClass().getClassLoader());
        assertThat(routing.targetOf(MANIFEST).name).isEqualTo("accel");
        assertThat(routing.targetOf(DATA_FILE).name).isEqualTo("cluster");
    }

    private static Options singleTargetOptions() {
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        options.set("io-cache.enabled", "true");
        options.set("io-cache.endpoint", "http://cache:8080");
        options.set("io-cache.origin.endpoint", "https://origin.example.com");
        options.set("io-cache.policy", "meta,read");
        return options;
    }

    private static Options twoTargetOptions() {
        Options options = singleTargetOptions();
        options.set("io-cache.targets", "accel,cluster");
        options.set("io-cache.target.accel.endpoint", "https://accel.example.com");
        options.set("io-cache.target.cluster.endpoint", "http://10.0.0.1:8080");
        options.set("io-cache.routes", "meta=accel;data,bucket-index=cluster");
        return options;
    }

    private static String host(String endpoint) {
        String host = endpoint.contains("://") ? endpoint.split("://", 2)[1] : endpoint;
        return host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    }

    private static FileType fileType(String type) {
        return FileType.valueOf(type.toUpperCase(Locale.ROOT).replace('-', '_'));
    }

    // exists is a status lookup; an atomic write stages a temp file and renames it on OSS
    private static String opType(String op) {
        if ("exists".equals(op)) {
            return "meta";
        }
        if ("atomic-write".equals(op)) {
            return "rename";
        }
        return op.endsWith("-write") ? "write" : op;
    }

    private static List<Arguments> endpointCases() throws IOException {
        JsonNode root = readCases("routing.json");
        List<Arguments> cases = new ArrayList<>();
        for (JsonNode c : root.get("cases")) {
            cases.add(
                    Arguments.of(
                            c.get("name").asText(),
                            options(c.get("options")),
                            c.get("op").asText(),
                            c.get("path").asText(),
                            c.get("expect").asText()));
        }
        assertThat(cases).isNotEmpty();
        return cases;
    }

    private static List<Arguments> fileTypeCases() throws IOException {
        List<Arguments> cases = new ArrayList<>();
        for (JsonNode c : readCases("routable-types.json").get("cases")) {
            JsonNode type = c.get("type");
            cases.add(
                    Arguments.of(
                            c.get("path").asText(),
                            options(c.get("options")),
                            type.isNull() ? null : type.asText(),
                            c.get("reason").asText()));
        }
        assertThat(cases).isNotEmpty();
        return cases;
    }

    private static Options options(JsonNode node) {
        Options options = new Options();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            options.set(field.getKey(), field.getValue().asText());
        }
        return options;
    }

    private static JsonNode readCases(String name) throws IOException {
        try (InputStream in =
                IoCacheRoutingTest.class.getClassLoader().getResourceAsStream("io-cache/" + name)) {
            assertThat(in).as(name).isNotNull();
            return JsonSerdeUtil.OBJECT_MAPPER_INSTANCE.readTree(in);
        }
    }
}
