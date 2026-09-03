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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HAUtilClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** Tests for the canonical and ownership contract of custom partition locations. */
class FormatTablePartitionPathResolverTest {

    private static final Path TABLE_PATH = new Path("file:/warehouse/table");
    private static final String TABLE_NAME = "db.table";

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                " ",
                " file:/archive/dt=2026",
                "file:/archive/dt=2026 ",
                "/tmp/archive",
                "relative/path",
                "oss:/archive",
                "oss:///archive",
                "oss://user@bucket/archive",
                "oss://bucket/",
                "file:/",
                "file://localhost/archive/dt=2026",
                "oss://bucket/archive?version=1",
                "oss://bucket/archive#fragment",
                "oss://bucket/archive\\child",
                "oss://bucket/a/./b",
                "oss://bucket/a/../b",
                "oss://bucket/a/%2e%2e/b",
                "oss://bucket/a%2f../b",
                "oss://bucket/a%2F%2e%2E/b",
                "oss://bucket/a/%252e%252e/b",
                "oss://bucket/a/%25252e%25252e/b",
                "oss://bucket/a/%2e%252e/b",
                "oss://bucket/archive/%2564t%253D2026",
                "oss://bucket/a%252f%252e%252e%252fb",
                "oss://bucket/a%25252f%25252e%25252e%25252fb",
                "oss://bucket/archive%5c..%5csecret",
                "oss://bucket/archive%3Fversion=1",
                "oss://bucket/archive%23fragment",
                "oss://bucket/archive%",
                "oss://bucket/archive%2",
                "oss://bucket/archive%GG"
            })
    void testRejectsInvalidCustomLocation(String location) {
        FormatTablePartitionPathResolver resolver = resolver();

        assertThatThrownBy(() -> resolver.resolve(spec(), location))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location")
                .hasMessageContaining(TABLE_NAME);
    }

    @Test
    void testRejectsRawControlCharacter() {
        String location = "oss://bucket/archive" + (char) 0 + "child";

        assertThatThrownBy(() -> resolver().resolve(spec(), location))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location")
                .hasMessageContaining(TABLE_NAME);
    }

    @Test
    void testRejectsNullPathOptionFromCatalog() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), null);
        Partition partition =
                new Partition(spec(), 0, 0, 0, 0, -1, false, null, null, null, null, options);

        assertThatThrownBy(
                        () ->
                                FormatTablePartitionRegistryValidator.validatePartitionLocations(
                                        Collections.singletonList(partition),
                                        Arrays.asList("year", "month"),
                                        TABLE_PATH,
                                        TABLE_NAME,
                                        false,
                                        null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("path option");
    }

    @Test
    void testCanonicalizesSchemeAuthoritySeparatorsAndPercentEncoding() {
        String location = "OSS://BUCKET//archive///%64t%3D2026%2Fmonth%3D09/";

        Path resolved = resolver().resolve(spec(), location);

        assertThat(resolved.toString()).isEqualTo("oss://bucket/archive/dt=2026/month=09");
    }

    @Test
    void testCanonicalizesFileLocationWithoutAuthority() {
        Path resolved = resolver().resolve(spec(), "FILE:///archive//dt=2026/");

        assertThat(resolved.toString()).isEqualTo("file:/archive/dt=2026");
    }

    @Test
    void testRejectsAuthoritylessHdfsLocationWithoutCatalogContext() {
        assertThatThrownBy(() -> resolver().resolve(spec(), "hdfs:///archive/dt=2026"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location")
                .hasRootCauseMessage(
                        "Authorityless HDFS location requires an HDFS default filesystem.");
    }

    @Test
    void testAcceptsAbfsFilesystemAuthorityButRejectsCredentials() {
        Path resolved =
                resolver()
                        .resolve(
                                spec(),
                                "ABFS://filesystem@ACCOUNT.dfs.core.windows.net/archive/dt=2026");

        assertThat(resolved.toString())
                .isEqualTo("abfs://filesystem@account.dfs.core.windows.net/archive/dt=2026");
        assertThat(
                        resolver()
                                .resolve(
                                        spec("secure"),
                                        "ABFSS://filesystem@ACCOUNT.dfs.core.windows.net/archive/dt=2026")
                                .toString())
                .isEqualTo("abfss://filesystem@account.dfs.core.windows.net/archive/dt=2026");
        assertThatThrownBy(
                        () ->
                                resolver()
                                        .resolve(
                                                spec(),
                                                "abfs://user:password@account.dfs.core.windows.net/archive/dt=2026"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
    }

    @Test
    void testAbfsAndAbfssShareOneOwnershipIdentity() {
        Path tableRoot =
                new Path("abfss://filesystem@account.dfs.core.windows.net/warehouse/table");
        FormatTablePartitionPathResolver tableResolver =
                new FormatTablePartitionPathResolver(tableRoot, TABLE_NAME, false);

        assertThatThrownBy(
                        () ->
                                tableResolver.resolve(
                                        spec(),
                                        "abfs://filesystem@account.dfs.core.windows.net/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path(
                                        "abfs://filesystem@account.dfs.core.windows.net/warehouse/table/dt=2026"),
                                tableRoot))
                .isTrue();

        FormatTablePartitionPathResolver ownership = resolver();
        LinkedHashMap<String, String> first = spec("first");
        LinkedHashMap<String, String> second = spec("second");
        assertThat(
                        ownership.validateAndRecord(
                                first,
                                ownership.resolve(
                                        first,
                                        "abfs://filesystem@account.dfs.core.windows.net/archive/shared")))
                .isTrue();
        assertThatThrownBy(
                        () ->
                                ownership.validateAndRecord(
                                        first,
                                        ownership.resolve(
                                                first,
                                                "abfss://filesystem@account.dfs.core.windows.net/archive/shared")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");
        assertThatThrownBy(
                        () ->
                                ownership.validateAndRecord(
                                        second,
                                        ownership.resolve(
                                                second,
                                                "abfss://filesystem@account.dfs.core.windows.net/archive/shared")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");
    }

    @Test
    void testRejectsDefaultHdfsPortAliasOfTableRoot() {
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(
                        new Path("hdfs://namenode:8020/warehouse/table"), TABLE_NAME, false);

        assertThatThrownBy(() -> resolver.resolve(spec(), "hdfs://namenode/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
    }

    @Test
    void testRejectsCustomLocationsInvolvingViewFsAliases() {
        FormatTablePartitionPathResolver viewFsTable =
                new FormatTablePartitionPathResolver(
                        new Path("viewfs://cluster/warehouse/table"), TABLE_NAME, false);

        assertThatThrownBy(
                        () -> viewFsTable.resolve(spec(), "hdfs://namenode:8020/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThatThrownBy(() -> resolver().resolve(spec(), "viewfs://cluster/archive/dt=2026"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
    }

    @Test
    void testResolvesAuthoritylessHdfsAgainstCatalogDefault() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("fs.defaultFS", "hdfs://localhost:8020");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(
                        new Path("hdfs://localhost:8020/warehouse/table"),
                        TABLE_NAME,
                        false,
                        context);

        Path resolved = resolver.resolve(spec("external"), "hdfs:///archive/dt=2026");
        assertThat(resolved.toUri().getAuthority()).isEqualTo("localhost:8020");
        assertThat(resolved.toUri().getPath()).isEqualTo("/archive/dt=2026");
        assertThatThrownBy(() -> resolver.resolve(spec(), "hdfs:///warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
    }

    @Test
    void testAuthoritylessHdfsCanonicalizationIsIdempotentForLogicalNameservice() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("fs.defaultFS", "hdfs://MYNS");
        hadoopConf.set("dfs.nameservices", "MYNS");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);

        Path canonical =
                FormatTablePartitionPathResolver.canonicalizeCustomLocation(
                        "hdfs:///archive/dt=2026", context);
        Path canonicalAgain =
                FormatTablePartitionPathResolver.canonicalizeCustomLocation(
                        canonical.toString(), context);

        assertThat(canonical.toString()).isEqualTo("hdfs://MYNS/archive/dt=2026");
        assertThat(canonicalAgain).isEqualTo(canonical);
        assertThat(HAUtilClient.isLogicalUri(hadoopConf, canonical.toUri())).isTrue();
    }

    @Test
    void testCaseAmbiguousLogicalNameservicesFailClosed() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("dfs.nameservices", "MYNS,myns");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);

        assertThatThrownBy(
                        () ->
                                FormatTablePartitionPathResolver.canonicalizeCustomLocation(
                                        "hdfs://MYNS/archive/dt=2026", context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple logical nameservices");
    }

    @Test
    void testLogicalNameservicePortAliasCannotBypassOwnership() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("dfs.nameservices", "mycluster");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);
        Path tableRoot = new Path("hdfs://mycluster/warehouse/table");
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(tableRoot, TABLE_NAME, false, context);

        assertThatThrownBy(() -> resolver.resolve(spec(), "hdfs://mycluster:8020/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("hdfs://mycluster:8020/warehouse/table/year=2025"),
                                tableRoot,
                                context))
                .isTrue();
    }

    @Test
    void testLogicalNameserviceMemberAliasCannotBypassOwnership() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("dfs.nameservices", "mycluster");
        hadoopConf.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
        hadoopConf.set("dfs.namenode.rpc-address.mycluster.nn1", "nn1:8020");
        hadoopConf.set("dfs.namenode.rpc-address.mycluster.nn2", "nn2:8020");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);
        Path tableRoot = new Path("hdfs://mycluster/warehouse/table");
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(tableRoot, TABLE_NAME, false, context);

        assertThatThrownBy(() -> resolver.resolve(spec(), "hdfs://nn1:8020/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("hdfs://nn1/warehouse/table/year=2025"),
                                tableRoot,
                                context))
                .isTrue();
        assertThat(
                        FormatTablePartitionPathResolver.canonicalizeCustomLocation(
                                        "hdfs://nn1:8020/archive/dt=2026", context)
                                .toString())
                .isEqualTo("hdfs://mycluster/archive/dt=2026");
    }

    @Test
    void testFederatedNameserviceMemberAliasCannotBypassOwnership() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("dfs.nameservices", "federated");
        hadoopConf.set("dfs.namenode.rpc-address.federated", "nn1:8020");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);
        Path tableRoot = new Path("hdfs://federated/warehouse/table");
        FormatTablePartitionPathResolver resolver =
                new FormatTablePartitionPathResolver(tableRoot, TABLE_NAME, false, context);

        assertThatThrownBy(() -> resolver.resolve(spec(), "hdfs://nn1:8020/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("hdfs://nn1/warehouse/table/year=2025"),
                                tableRoot,
                                context))
                .isTrue();
    }

    @Test
    void testHdfsCanonicalIdentityIsUsedForFileIoRouting() {
        Configuration hadoopConf = new Configuration(false);
        hadoopConf.set("fs.defaultFS", "hdfs://localhost:8020");
        CatalogContext context = CatalogContext.create(new Options(), hadoopConf);
        Path tableRoot = new Path("hdfs://localhost:8020/warehouse/table");

        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("hdfs://localhost/warehouse/table/year=2025"),
                                tableRoot,
                                context))
                .isTrue();
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("hdfs:///warehouse/table/year=2025"), tableRoot, context))
                .isTrue();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "file:/warehouse/table",
                "FILE:///warehouse//table/",
                "file:/warehouse/table/custom/location",
                "file:/warehouse/table/year=2025",
                "file:/warehouse/table/year=2025/month=11",
                "file:/warehouse/table/year=2025/month=11/subdir",
                "file:/warehouse/table/year%3D2025%2Fmonth%3D11",
                "file:/warehouse"
            })
    void testRejectsLocationOwnedByTableOrDefaultPartition(String location) {
        assertThatThrownBy(() -> resolver().resolve(spec(), location))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location")
                .hasMessageContaining(TABLE_NAME);
    }

    @Test
    void testS3SchemeAliasesShareOneOwnershipIdentity() {
        Path tableRoot = new Path("s3://bucket/warehouse/table");
        FormatTablePartitionPathResolver tableResolver =
                new FormatTablePartitionPathResolver(tableRoot, TABLE_NAME, false);

        assertThatThrownBy(() -> tableResolver.resolve(spec(), "s3a://bucket/warehouse/table"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("invalid custom location");
        assertThat(
                        FormatTablePartitionPathResolver.isWithin(
                                new Path("s3n://bucket/warehouse/table/year=2025"), tableRoot))
                .isTrue();

        FormatTablePartitionPathResolver ownership = resolver();
        LinkedHashMap<String, String> first = spec("first");
        LinkedHashMap<String, String> second = spec("second");
        assertThat(
                        ownership.validateAndRecord(
                                first, ownership.resolve(first, "s3a://bucket/archive/shared")))
                .isTrue();
        assertThatThrownBy(
                        () ->
                                ownership.validateAndRecord(
                                        second,
                                        ownership.resolve(second, "s3n://bucket/archive/shared")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");
    }

    @Test
    void testOwnershipValidationScalesToLargePartitionRegistries() {
        assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () -> {
                    FormatTablePartitionPathResolver resolver = resolver();
                    for (int partition = 0; partition < 25_000; partition++) {
                        LinkedHashMap<String, String> spec = spec("month-" + partition);
                        Path path =
                                resolver.resolve(
                                        spec, "oss://bucket/archive/partition-" + partition);
                        assertThat(resolver.validateAndRecord(spec, path)).isTrue();
                    }
                });
    }

    @Test
    void testPathSegmentBoundariesDistinguishPrefixesFromAncestors() {
        FormatTablePartitionPathResolver resolver = resolver();
        LinkedHashMap<String, String> first = spec("first");
        LinkedHashMap<String, String> second = spec("second");
        LinkedHashMap<String, String> child = spec("child");

        assertThat(
                        resolver.validateAndRecord(
                                first, resolver.resolve(first, "oss://bucket/archive/a-b")))
                .isTrue();
        assertThat(
                        resolver.validateAndRecord(
                                second, resolver.resolve(second, "oss://bucket/archive/a")))
                .isTrue();
        assertThatThrownBy(
                        () ->
                                resolver.validateAndRecord(
                                        child,
                                        resolver.resolve(child, "oss://bucket/archive/a/child")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");

        FormatTablePartitionPathResolver reverseOrder = resolver();
        LinkedHashMap<String, String> descendant = spec("descendant");
        LinkedHashMap<String, String> ancestor = spec("ancestor");
        assertThat(
                        reverseOrder.validateAndRecord(
                                descendant,
                                reverseOrder.resolve(
                                        descendant, "oss://bucket/archive/root/child")))
                .isTrue();
        assertThatThrownBy(
                        () ->
                                reverseOrder.validateAndRecord(
                                        ancestor,
                                        reverseOrder.resolve(
                                                ancestor, "oss://bucket/archive/root")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("overlapping locations");
    }

    private static FormatTablePartitionPathResolver resolver() {
        return new FormatTablePartitionPathResolver(TABLE_PATH, TABLE_NAME, false);
    }

    private static LinkedHashMap<String, String> spec() {
        return spec("11");
    }

    private static LinkedHashMap<String, String> spec(String month) {
        LinkedHashMap<String, String> spec = new LinkedHashMap<>();
        spec.put("year", "2025");
        spec.put("month", month);
        return spec;
    }
}
