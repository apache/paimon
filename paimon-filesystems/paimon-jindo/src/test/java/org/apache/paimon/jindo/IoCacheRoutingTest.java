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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.FileType.BUCKET_INDEX;
import static org.apache.paimon.utils.FileType.DATA;
import static org.apache.paimon.utils.FileType.FILE_INDEX;
import static org.apache.paimon.utils.FileType.GLOBAL_INDEX;
import static org.apache.paimon.utils.FileType.META;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IoCacheRouting}. */
public class IoCacheRoutingTest {

    private static final String TABLE_ROOT = "oss://bkt/db1.db/t1";
    private static final String UUID = "8b1f7c2e-3a4d-4e5f-9a0b-1c2d3e4f5a6b";
    private static final Path DATA_FILE =
            new Path(TABLE_ROOT + "/dt=2026-09-30/bucket-0/data-" + UUID + "-0.orc");
    private static final Path MANIFEST = new Path(TABLE_ROOT + "/manifest/manifest-" + UUID + "-0");

    // paths below are relative to TABLE_ROOT, and {uuid} stands for UUID
    private static final String DATA_PATH = "dt=1/bucket-0/data-{uuid}-0.parquet";
    private static final String MANIFEST_PATH = "manifest/manifest-{uuid}-0";
    private static final String INDEX_PATH = "index/index-{uuid}-0";
    private static final String GLOBAL_INDEX_PATH = "index/btree-global-index-{uuid}.index";
    private static final String TEMP_PATH = "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp";
    private static final String SNAPSHOT_PATH = "snapshot/snapshot-12";
    private static final String LATEST_PATH = "snapshot/LATEST";

    private static final String OSS = "https://oss-cn-hangzhou-internal.aliyuncs.com";
    private static final String CACHE = "http://cache.example.com";
    private static final String ACCEL = "https://accelerator.example.com";
    private static final String CLUSTER = "http://cluster.example.com";
    private static final String WRITE = "io-cache.policy=meta,read,write";
    private static final String EXISTS = "io-cache.policy=meta,read,exists";

    @Test
    public void testOneCacheTarget() {
        Options options = single();
        assertEndpoint(options, "read", DATA_PATH, CACHE);
        assertEndpoint(options, "meta", DATA_PATH, CACHE);
        // exists asks whether a file is still there, so it needs its own policy token
        assertEndpoint(options, "exists", DATA_PATH, OSS);
        assertEndpoint(single(EXISTS), "exists", DATA_PATH, CACHE);
        assertEndpoint(single(EXISTS), "exists", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "read", MANIFEST_PATH, CACHE);
        assertEndpoint(options, "read", "manifest/manifest-list-{uuid}-1", CACHE);
        assertEndpoint(options, "read", "oss://other-bkt/db1.db/t1/" + DATA_PATH, CACHE);
        assertEndpoint(options, "read", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "meta", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "exists", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "read", LATEST_PATH, OSS);
        assertEndpoint(options, "exists", LATEST_PATH, OSS);
        assertEndpoint(options, "read", TEMP_PATH, OSS);
        assertEndpoint(options, "read", "dt=1/bucket-0/000000_0", OSS);
        assertEndpoint(options, "read", "dls://bkt/db1.db/t1/" + DATA_PATH, OSS);
        // the whitelist has no index
        assertEndpoint(options, "read", INDEX_PATH, OSS);
        // a Format Table file named like a manifest may be replaced in place
        assertEndpoint(options, "read", "review_external/manifest.parquet", OSS);
        assertEndpoint(options, "meta", "review_external/manifest.parquet", OSS);
        assertEndpoint(options, "write", DATA_PATH, OSS);
        for (String op : Arrays.asList("list", "delete", "rename", "mkdirs")) {
            assertEndpoint(options, op, DATA_PATH, OSS);
        }
    }

    @Test
    public void testPolicyAndWhitelist() {
        assertEndpoint(single("-io-cache.enabled"), "read", DATA_PATH, CACHE);
        assertEndpoint(single("io-cache.enabled=false"), "exists", DATA_PATH, CACHE);
        assertEndpoint(single("-io-cache.policy"), "exists", DATA_PATH, CACHE);
        assertEndpoint(single("io-cache.policy=none"), "exists", DATA_PATH, CACHE);

        Options readOnly = single("io-cache.policy=read");
        assertEndpoint(readOnly, "read", DATA_PATH, CACHE);
        assertEndpoint(readOnly, "meta", DATA_PATH, OSS);
        assertEndpoint(readOnly, "exists", DATA_PATH, OSS);
        Options writeOnly = single("io-cache.policy=write");
        assertEndpoint(writeOnly, "write", DATA_PATH, CACHE);
        assertEndpoint(writeOnly, "exists", DATA_PATH, OSS);
        Options existsOnly = single("io-cache.policy=exists");
        assertEndpoint(existsOnly, "exists", DATA_PATH, CACHE);
        assertEndpoint(existsOnly, "meta", DATA_PATH, OSS);

        assertEndpoint(single("-io-cache.whitelist"), "read", INDEX_PATH, CACHE);
        assertEndpoint(single("io-cache.whitelist=*"), "read", GLOBAL_INDEX_PATH, CACHE);
        assertEndpoint(single("io-cache.whitelist=*"), "read", DATA_PATH + ".index", CACHE);
    }

    @Test
    public void testEndpoints() {
        assertEndpoint(single("io-cache.endpoint=  "), "read", DATA_PATH, OSS);
        assertEndpoint(
                single("io-cache.endpoint=", "io-cache.routes=*=default"), "read", DATA_PATH, OSS);
        // the origin defaults to fs.oss.endpoint
        String oss = "fs.oss.endpoint=" + OSS;
        assertEndpoint(single(oss, "-io-cache.origin.endpoint"), "list", DATA_PATH, OSS);
        assertEndpoint(single(oss, "io-cache.origin.endpoint=  "), "list", DATA_PATH, OSS);
        // a client-side endpoint turns routing off
        String override = "https://oss-cn-hangzhou.aliyuncs.com";
        assertEndpoint(single("dlf.oss-endpoint=" + override), "read", DATA_PATH, override);
    }

    @Test
    public void testWritePolicy() {
        Options options = single(WRITE);
        assertEndpoint(options, "write", DATA_PATH, CACHE);
        assertEndpoint(options, "write", MANIFEST_PATH, CACHE);
        assertEndpoint(options, "write", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "write", LATEST_PATH, OSS);
        assertEndpoint(options, "write", TEMP_PATH, OSS);
        for (String op : Arrays.asList("list", "delete", "rename", "mkdirs")) {
            assertEndpoint(options, op, DATA_PATH, OSS);
        }
        assertEndpoint(single(WRITE, "io-cache.whitelist=meta"), "write", DATA_PATH, OSS);

        assertEndpoint(multi(WRITE), "write", DATA_PATH, CLUSTER);
        assertEndpoint(multi(WRITE), "write", MANIFEST_PATH, ACCEL);
    }

    @Test
    public void testTwoCacheTargets() {
        Options options = multi();
        assertEndpoint(options, "read", MANIFEST_PATH, ACCEL);
        assertEndpoint(options, "meta", MANIFEST_PATH, ACCEL);
        assertEndpoint(options, "exists", MANIFEST_PATH, OSS);
        assertEndpoint(multi(EXISTS), "exists", MANIFEST_PATH, ACCEL);
        assertEndpoint(options, "read", DATA_PATH, CLUSTER);
        assertEndpoint(multi(EXISTS), "exists", DATA_PATH, CLUSTER);
        assertEndpoint(options, "read", INDEX_PATH, CLUSTER);
        assertEndpoint(options, "read", GLOBAL_INDEX_PATH, CLUSTER);
        assertEndpoint(options, "read", SNAPSHOT_PATH, OSS);
        assertEndpoint(options, "write", MANIFEST_PATH, OSS);
        for (String op : Arrays.asList("list", "delete", "rename", "mkdirs")) {
            assertEndpoint(options, op, MANIFEST_PATH, OSS);
        }
        assertEndpoint(multi("io-cache.whitelist=meta"), "read", DATA_PATH, OSS);
    }

    @Test
    public void testTargetsAndRoutes() {
        // without routes the first target with an endpoint takes every type
        String noRoutes = "-io-cache.routes";
        assertEndpoint(multi(noRoutes), "read", DATA_PATH, ACCEL);
        assertEndpoint(multi(noRoutes, "io-cache.endpoint=" + CACHE), "read", DATA_PATH, ACCEL);
        assertEndpoint(
                multi(noRoutes, "-io-cache.target.accel.endpoint"), "read", DATA_PATH, CLUSTER);
        assertEndpoint(
                multi(noRoutes, "io-cache.target.accel.endpoint=  "), "read", DATA_PATH, CLUSTER);
        assertEndpoint(
                multi(noRoutes, "io-cache.targets=Bad_Name,cluster"), "read", DATA_PATH, CLUSTER);
        assertEndpoint(
                multi("io-cache.target.cluster.endpoint=  " + CLUSTER + "  "),
                "read",
                DATA_PATH,
                CLUSTER);

        // a type without a usable route goes to the origin
        assertEndpoint(multi("io-cache.routes=data=x"), "read", DATA_PATH, OSS);
        assertEndpoint(multi("-io-cache.target.cluster.endpoint"), "read", DATA_PATH, OSS);
        String metaAndData = "io-cache.routes=meta=accel;data=cluster";
        assertEndpoint(multi(metaAndData), "read", DATA_PATH + ".index", OSS);

        assertEndpoint(multi("io-cache.routes=*=cluster"), "read", MANIFEST_PATH, CLUSTER);
        String firstWins = "io-cache.routes=data=accel;data,meta=cluster";
        assertEndpoint(multi(firstWins), "read", DATA_PATH, ACCEL);
        String malformed = "io-cache.routes==cluster;meta=accel;junk";
        assertEndpoint(multi(malformed), "read", MANIFEST_PATH, ACCEL);
        assertEndpoint(multi(malformed), "read", DATA_PATH, OSS);
    }

    @Test
    public void testDataFilesNamedLikeMetadata() {
        Options manifest = multi("data-file.prefix=manifest-");
        assertEndpoint(manifest, "read", "dt=1/bucket-0/manifest-{uuid}-0.orc", CLUSTER);
        assertEndpoint(manifest, "read", "dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc", CLUSTER);
        assertEndpoint(manifest, "read", MANIFEST_PATH, ACCEL);
        assertEndpoint(manifest, "read", MANIFEST_PATH + ".avro.sidecar", ACCEL);
        Options snapshot = multi("data-file.prefix=snapshot-");
        assertEndpoint(snapshot, "read", "dt=1/bucket-0/snapshot-{uuid}-0.orc", CLUSTER);
        Options stat = multi("data-file.prefix=stat-");
        assertEndpoint(stat, "read", "dt=1/bucket-0/stat-{uuid}-0.orc", CLUSTER);
        Options index =
                multi("io-cache.routes=data=cluster;bucket-index=accel", "data-file.prefix=index-");
        assertEndpoint(index, "read", "dt=1/bucket-0/index-{uuid}-0.orc", CLUSTER);
        assertEndpoint(index, "read", INDEX_PATH, ACCEL);

        String custom = "dt=1/bucket-0/custom-{uuid}-0.orc";
        assertEndpoint(single(), "read", custom, OSS);
        assertEndpoint(single("data-file.prefix=custom-"), "read", custom, CACHE);
    }

    @Test
    public void testCacheableType() {
        // named by sequence id or rewritten in place
        for (String path :
                Arrays.asList(
                        "snapshot/snapshot-12",
                        "snapshot/LATEST",
                        "snapshot/EARLIEST",
                        "branch/branch-dev/snapshot/LATEST",
                        "schema/schema-3",
                        "changelog/changelog-5",
                        "changelog/LATEST",
                        "tag/tag-2026-09-30",
                        "tag/tag-success-file/t1_SUCCESS",
                        "consumer/consumer-job1",
                        "service/service-primary-key-lookup",
                        "dt=1/_SUCCESS",
                        "oss://bkt/bucket-0/db/t/changelog/changelog-5")) {
            assertType(path, null);
        }
        // temporary or not written by Paimon
        for (String path :
                Arrays.asList(
                        "snapshot/.snapshot-13.{uuid}.tmp",
                        "dt=1/bucket-0/.data-{uuid}-0.parquet.{uuid}.tmp",
                        "dt=1/bucket-0/data-{uuid}-0.parquet.tmp-{uuid}",
                        "dt=1/bucket-0/data-{uuid}-0.parquet.tmp.{uuid}",
                        "metadata/version-hint.text",
                        "metadata/v3.metadata.json",
                        "dt=1/bucket-0/000000_0",
                        "dt=1/part-00000-1a2b.snappy.parquet",
                        "dt=1/bucket-0/data-1.parquet",
                        "dt=1/bucket-0/custom-{uuid}-0.orc",
                        "README")) {
            assertType(path, null);
        }
        // Format Table files named like metadata or indexes, which may be replaced in place
        for (String path :
                Arrays.asList(
                        "dt=1/manifest.parquet",
                        "dt=1/stat-2024.parquet",
                        "dt=1/index-a.csv",
                        "dt=1/foo.index",
                        "review_external/manifest.parquet",
                        "manifest/manifest-old",
                        "manifest/manifest-old.avro.sidecar",
                        "manifest/manifest-list-{uuid}-1.avro.sidecar",
                        "statistics/stat-old",
                        "index/index-old",
                        "index/my-global-index.index",
                        "index/btree-global-index-{uuid}-0.index")) {
            assertType(path, null);
        }

        assertType("manifest/manifest-{uuid}-0", META);
        assertType("manifest/manifest-list-{uuid}-1", META);
        assertType("manifest/index-manifest-{uuid}-0", META);
        assertType("manifest/manifest-{uuid}-0.avro.sidecar", META);
        assertType("statistics/stat-{uuid}-0", META);
        assertType("dt=1/bucket-0/data-{uuid}-0.parquet", DATA);
        assertType("dt=1/bucket-0/changelog-{uuid}-0.parquet", DATA);
        assertType("dt=1/bucket-0/data-{uuid}-0.blob", DATA);
        assertType("dt=1/8b1f7c2e/data-{uuid}-0.parquet", DATA);
        assertType("dt=1/bucket-0/data-{uuid}-0.parquet.index", FILE_INDEX);
        assertType("index/index-{uuid}-0", BUCKET_INDEX);
        assertType("index/btree-global-index-{uuid}.index", GLOBAL_INDEX);
        assertType("index/lumina-global-index-{uuid}.index", GLOBAL_INDEX);
    }

    @Test
    public void testCacheableTypeWithFilePrefixes() {
        assertType("dt=1/bucket-0/custom-{uuid}-0.orc", DATA, "data-file.prefix=custom-");
        assertType("dt=1/bucket-0/data-{uuid}-0.orc", DATA, "data-file.prefix=custom-");
        assertType("dt=1/bucket-0/cl-{uuid}-0.orc", DATA, "changelog-file.prefix=cl-");
        assertType("dt=1/bucket-0/  unknown.parquet", null, "data-file.prefix=  ");

        // the name decides, so data files may be named like metadata
        String manifest = "data-file.prefix=manifest-";
        assertType("dt=1/bucket-0/manifest-{uuid}-0.orc", DATA, manifest);
        assertType("dt=1/bucket-0/7f3a/manifest-{uuid}-0.orc", DATA, manifest);
        assertType("dt=1/bucket-0/manifest-{uuid}-0.orc.index", FILE_INDEX, manifest);
        assertType("manifest/manifest-{uuid}-0", META, manifest);
        assertType("manifest/manifest-{uuid}-0.avro.sidecar", META, manifest);
        String index = "data-file.prefix=index-";
        assertType("dt=1/bucket-0/index-{uuid}-0.orc", DATA, index);
        assertType("bucket-postpone/index-{uuid}-0.orc", DATA, index);
        assertType("index/index-{uuid}-0", BUCKET_INDEX, index);
        assertType("dt=1/bucket-0/index-{uuid}-0", BUCKET_INDEX, index);
        String stat = "data-file.prefix=stat-";
        assertType("dt=1/bucket-0/stat-{uuid}-0.orc", DATA, stat);
        assertType("postpone/stat-{uuid}-0.orc", DATA, stat);
        assertType("statistics/stat-{uuid}-0", META, stat);
        String snapshot = "data-file.prefix=snapshot-";
        assertType("dt=1/bucket-0/snapshot-{uuid}-0.orc", DATA, snapshot);
        assertType("dt=1/bucket-0/snapshot-{uuid}-0.orc.index", FILE_INDEX, snapshot);
        assertType("dt=1/bucket-0/schema-{uuid}-0.orc", DATA, "data-file.prefix=schema-");
        assertType(
                "dt=1/bucket-0/global-index-{uuid}-0.orc.index",
                FILE_INDEX,
                "data-file.prefix=global-index-");

        // names rewritten in place never use a cache, whatever the prefix
        assertType("dt=1/bucket-0/tag-{uuid}-0.orc", null, "data-file.prefix=tag-");
        assertType("dt=1/bucket-0/consumer-{uuid}-0.orc", null, "data-file.prefix=consumer-");
        String warehouse = "oss://bkt/warehouse/bucket-0/db/t1/";
        assertType(warehouse + "tag/tag-file", null, "data-file.prefix=tag-");
        assertType(warehouse + "consumer/consumer-file", null, "data-file.prefix=consumer-");
        assertType(warehouse + "service/service-file", null, "data-file.prefix=service-");
        assertType(warehouse + "metadata/version-hint.text", null, "data-file.prefix=version-");
        assertType(warehouse + "branch/branch-feature", null, "data-file.prefix=branch-");
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
        return Arrays.asList(
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

    /** One cache target, which older clients also get as fs.oss.endpoint. */
    private static Options single(String... changes) {
        Options options = new Options();
        options.set("fs.oss.endpoint", CACHE);
        options.set("io-cache.enabled", "true");
        options.set("io-cache.endpoint", CACHE);
        options.set("io-cache.origin.endpoint", OSS);
        options.set("io-cache.policy", "meta,read");
        options.set("io-cache.whitelist", "meta,data");
        return with(options, changes);
    }

    /** Metadata on an accelerator, data and indexes on a cache cluster. */
    private static Options multi(String... changes) {
        Options options = new Options();
        options.set("fs.oss.endpoint", ACCEL);
        options.set("io-cache.enabled", "true");
        options.set("io-cache.origin.endpoint", OSS);
        options.set("io-cache.targets", "accel,cluster");
        options.set("io-cache.target.accel.endpoint", ACCEL);
        options.set("io-cache.target.accel.region", "cn-hangzhou");
        options.set("io-cache.target.cluster.endpoint", CLUSTER);
        options.set("io-cache.target.cluster.path-style-access", "true");
        options.set("io-cache.policy", "meta,read");
        options.set("io-cache.whitelist", "*");
        options.set(
                "io-cache.routes", "meta=accel;data,bucket-index,global-index,file-index=cluster");
        return with(options, changes);
    }

    // "key=value" sets an option and "-key" removes it
    private static Options with(Options options, String... changes) {
        for (String change : changes) {
            if (change.startsWith("-")) {
                options.remove(change.substring(1));
            } else {
                int eq = change.indexOf('=');
                options.set(change.substring(0, eq), change.substring(eq + 1));
            }
        }
        return options;
    }

    private static void assertEndpoint(Options options, String op, String path, String expect) {
        Options copy = new Options(options.toMap());
        // RESTTokenFileIO puts a client-side dlf.oss-endpoint into fs.oss.endpoint
        String override = copy.get("dlf.oss-endpoint");
        if (override != null) {
            copy.set("fs.oss.endpoint", override);
        }
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(copy));
        assertThat(host(fileIO.hadoopOptions(path(path), op).get("fs.oss.endpoint")))
                .as("%s %s with %s", op, path, options.toMap())
                .isEqualTo(host(expect));
    }

    private static void assertType(String path, FileType type, String... changes) {
        List<String> prefixes = IoCacheRouting.dataFilePrefixes(with(new Options(), changes));
        assertThat(IoCacheRouting.cacheableType(path(path), prefixes))
                .as("%s with %s", path, Arrays.toString(changes))
                .isEqualTo(type);
    }

    private static Path path(String path) {
        String resolved = path.replace("{uuid}", UUID);
        return new Path(resolved.contains("://") ? resolved : TABLE_ROOT + "/" + resolved);
    }

    private static String host(String endpoint) {
        String host = endpoint.contains("://") ? endpoint.split("://", 2)[1] : endpoint;
        return host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    }
}
