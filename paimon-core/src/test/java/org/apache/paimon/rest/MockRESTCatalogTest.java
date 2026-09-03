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

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.TableType;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.management.ListPermissionsRequest;
import org.apache.paimon.management.PermissionAssignment;
import org.apache.paimon.management.PermissionColumns;
import org.apache.paimon.management.PermissionResource;
import org.apache.paimon.management.ResourceType;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.DLFAuthProvider;
import org.apache.paimon.rest.auth.DLFDefaultSigner;
import org.apache.paimon.rest.auth.DLFToken;
import org.apache.paimon.rest.auth.DLFTokenLoader;
import org.apache.paimon.rest.auth.DLFTokenLoaderFactory;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.requests.CreatePartitionsRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.BlobDescriptorReaderFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.paimon.catalog.Catalog.TABLE_DEFAULT_OPTION_PREFIX;
import static org.apache.paimon.rest.RESTApi.HEADER_PREFIX;
import static org.apache.paimon.rest.RESTApi.READ_VIA_HEADER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Test REST Catalog on Mocked REST server. */
class MockRESTCatalogTest extends RESTCatalogTest {

    private RESTCatalogServer restCatalogServer;
    private final String serverDefineHeaderName = "test-header";
    private final String serverDefineHeaderValue = "test-value";
    private String dataPath;
    private AuthProvider authProvider;
    private Map<String, String> authMap;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataPath = warehouse;
        String initToken = "init_token";
        this.authProvider = new BearTokenAuthProvider(initToken);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN.key(),
                        initToken,
                        RESTCatalogOptions.TOKEN_PROVIDER.key(),
                        AuthProviderEnum.BEAR.identifier());
        this.restCatalog = initCatalog(false);
        this.catalog = restCatalog;

        // test retry commit
        RESTCatalogServer.commitSuccessThrowException = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (restCatalogServer != null) {
            restCatalogServer.shutdown();
        }
    }

    @Test
    void testAuthFail() {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        options.set(RESTCatalogOptions.TOKEN, "aaaaa");
        options.set(RESTCatalogOptions.TOKEN_PROVIDER, AuthProviderEnum.BEAR.identifier());
        options.set(CatalogOptions.METASTORE, RESTCatalogFactory.IDENTIFIER);
        assertThatThrownBy(() -> new RESTCatalog(CatalogContext.create(options)))
                .isInstanceOf(NotAuthorizedException.class);
    }

    @Test
    void testRejectedSystemTablePatternKeepsWhatRaisedIt() {
        // the message is the argument, never the format, and the cause is what says where it
        // came from -- the caller sees only what this exception carries
        assertThatThrownBy(
                        () ->
                                catalog.listTablesPaged(
                                        SYSTEM_DATABASE_NAME, null, null, "a%b", null))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("prefix sql like pattern")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInvalidManagementDtoReturnsBadRequest() throws Exception {
        String database = "invalid_management_dto";
        Identifier identifier = Identifier.create(database, "orders");
        catalog.createDatabase(database, false);
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.QUERY_AUTH_ENABLED.key(), "true")
                        .build(),
                false);

        BadRequestException error =
                assertThrows(
                        BadRequestException.class,
                        () ->
                                new HttpClient(restCatalogServer.getUrl())
                                        .post(
                                                new ResourcePaths("paimon").grantPermission(),
                                                new InvalidColumnGrantRequest(identifier),
                                                restCatalog.api().authFunction()));

        assertThat(error).hasMessageContaining("columns is required for COLUMN resource");
    }

    @Test
    void testCreateResourceRejectsDatabaseMismatch() throws Exception {
        String pathDatabase = "path_database";
        String bodyDatabase = MockRESTMessage.databaseName();
        catalog.createDatabase(pathDatabase, true);
        catalog.createDatabase(bodyDatabase, true);
        HttpClient client = new HttpClient(restCatalogServer.getUrl());
        ResourcePaths paths = new ResourcePaths("paimon");

        BadRequestException tableError =
                assertThrows(
                        BadRequestException.class,
                        () ->
                                client.post(
                                        paths.tables(pathDatabase),
                                        MockRESTMessage.createTableRequest("orders"),
                                        restCatalog.api().authFunction()));
        BadRequestException viewError =
                assertThrows(
                        BadRequestException.class,
                        () ->
                                client.post(
                                        paths.views(pathDatabase),
                                        MockRESTMessage.createViewRequest("orders_view"),
                                        restCatalog.api().authFunction()));

        assertThat(tableError)
                .hasMessageContaining(
                        "The database in the table identifier must match the request path");
        assertThat(viewError)
                .hasMessageContaining(
                        "The database in the view identifier must match the request path");
        assertThat(catalog.listTables(bodyDatabase)).isEmpty();
        assertThat(catalog.listViews(bodyDatabase)).isEmpty();
    }

    @Test
    void testRenamePreservesHighestFieldIdForColumnPermissions() throws Exception {
        String principal = "analyst";
        Identifier source = Identifier.create("schema_lifecycle_db", "orders");
        Identifier destination = Identifier.create("schema_lifecycle_db", "renamed_orders");
        catalog.createDatabase(source.getDatabaseName(), false);
        catalog.createTable(
                source,
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "removed", DataTypes.STRING()),
                                new DataField(2, "secret", DataTypes.STRING())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.QUERY_AUTH_ENABLED.key(), "true"),
                        null),
                false);
        restCatalogServer.registerManagementPrincipal(principal);
        restCatalog
                .permissionManagement()
                .grantPermission(
                        new PermissionAssignment(
                                new PermissionResource(
                                        ResourceType.COLUMN,
                                        source.getDatabaseName(),
                                        source.getTableName(),
                                        null,
                                        null),
                                "SELECT",
                                principal,
                                new PermissionColumns(Collections.singletonList("id"), null),
                                null));

        catalog.alterTable(
                source, Collections.singletonList(SchemaChange.dropColumn("removed")), false);
        catalog.renameTable(source, destination, false);
        catalog.alterTable(
                destination,
                Collections.singletonList(SchemaChange.addColumn("new_column", DataTypes.STRING())),
                false);

        assertThat(catalog.getTable(destination).rowType().getFields())
                .extracting(DataField::id)
                .containsExactly(0, 2, 3);
    }

    @Test
    void testDropDatabaseRemovesViewsFunctionsAndAssignments() throws Exception {
        String database = "cascade_management_db";
        String principal = "analyst";
        Identifier view = Identifier.create(database, "orders_view");
        Identifier function = Identifier.create(database, "orders_function");
        catalog.createDatabase(database, false);
        catalog.createView(view, createView(view), false);
        catalog.createFunction(function, MockRESTMessage.function(function), false);
        restCatalogServer.registerManagementPrincipal(principal);
        PermissionResource viewResource =
                new PermissionResource(
                        ResourceType.VIEW, database, null, null, view.getObjectName());
        PermissionResource functionResource =
                new PermissionResource(
                        ResourceType.FUNCTION, database, null, function.getObjectName(), null);
        restCatalog
                .permissionManagement()
                .grantPermission(new PermissionAssignment(viewResource, "SELECT", principal, null));
        restCatalog
                .permissionManagement()
                .grantPermission(
                        new PermissionAssignment(functionResource, "SELECT", principal, null));

        catalog.dropDatabase(database, false, true);
        catalog.createDatabase(database, false);

        assertThat(catalog.listViews(database)).isEmpty();
        assertThat(catalog.listFunctions(database)).isEmpty();
        catalog.createView(view, createView(view), false);
        catalog.createFunction(function, MockRESTMessage.function(function), false);
        assertThat(
                        restCatalog
                                .permissionManagement()
                                .listPermissions(
                                        new ListPermissionsRequest(
                                                ResourceType.VIEW,
                                                database,
                                                null,
                                                null,
                                                view.getObjectName(),
                                                null,
                                                null,
                                                null,
                                                null))
                                .getElements())
                .isEmpty();
        assertThat(
                        restCatalog
                                .permissionManagement()
                                .listPermissions(
                                        new ListPermissionsRequest(
                                                ResourceType.FUNCTION,
                                                database,
                                                null,
                                                function.getObjectName(),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null))
                                .getElements())
                .isEmpty();
    }

    @Test
    void testDlfStSTokenAuth() throws Exception {
        String akId = "akId" + UUID.randomUUID();
        String akSecret = "akSecret" + UUID.randomUUID();
        String securityToken = "securityToken" + UUID.randomUUID();
        String uri = "https://cn-hangzhou-vpc.dlf.aliyuncs.com";
        String region = "cn-hangzhou";
        DLFToken dlfToken = new DLFToken(akId, akSecret, securityToken, null);
        this.authProvider = new TestDLFAuthProvider(dlfToken, uri, region);
        this.authMap =
                ImmutableMap.of(
                        RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.DLF.identifier(),
                        RESTCatalogOptions.DLF_REGION.key(), region,
                        RESTCatalogOptions.DLF_ACCESS_KEY_ID.key(), akId,
                        RESTCatalogOptions.DLF_ACCESS_KEY_SECRET.key(), akSecret,
                        RESTCatalogOptions.DLF_SECURITY_TOKEN.key(), securityToken);
        RESTCatalog restCatalog = initCatalog(false);
        testDlfAuth(restCatalog);
    }

    @Test
    void testDlfStSTokenPathAuth() throws Exception {
        String uri = "https://cn-hangzhou-vpc.dlf.aliyuncs.com";
        String region = "cn-hangzhou";
        java.nio.file.Path tokenFile =
                Paths.get(URI.create(dataPath)).resolve(UUID.randomUUID().toString());
        String tokenPath = tokenFile.toString();
        try {
            generateTokenAndWriteToFile(tokenPath);
            DLFTokenLoader tokenLoader =
                    DLFTokenLoaderFactory.createDLFTokenLoader(
                            "local_file",
                            new Options(
                                    ImmutableMap.of(
                                            RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath)));
            DLFToken dlfToken = tokenLoader.loadToken();
            this.authProvider = new TestDLFAuthProvider(dlfToken, uri, region);
            this.authMap =
                    ImmutableMap.of(
                            RESTCatalogOptions.TOKEN_PROVIDER.key(),
                            AuthProviderEnum.DLF.identifier(),
                            RESTCatalogOptions.DLF_REGION.key(),
                            region,
                            RESTCatalogOptions.DLF_TOKEN_PATH.key(),
                            tokenPath);
            RESTCatalog restCatalog = initCatalog(false);
            testDlfAuth(restCatalog);
        } finally {
            Files.deleteIfExists(tokenFile);
        }
    }

    @Test
    void testHeader() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("k1", "v1");
        parameters.put("k2", "v2");
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.api().authFunction().apply(restAuthParameter);
        assertEquals(
                headers.get(BearTokenAuthProvider.AUTHORIZATION_HEADER_KEY), "Bearer init_token");
        assertEquals(headers.get(serverDefineHeaderName), serverDefineHeaderValue);
    }

    @Test
    void testHeaderOptions() throws Exception {
        options.set(HEADER_PREFIX + "User-Agent", "test");
        RESTCatalog restCatalog = initCatalog(false);

        Map<String, String> parameters = new HashMap<>();
        RESTAuthParameter restAuthParameter =
                new RESTAuthParameter("/path", parameters, "method", "data");
        Map<String, String> headers = restCatalog.api().authFunction().apply(restAuthParameter);
        assertEquals(headers.get("User-Agent"), "test");

        RESTCatalog restCatalog2 = restCatalog.catalogLoader().load();
        Map<String, String> headers2 = restCatalog2.api().authFunction().apply(restAuthParameter);
        assertEquals(headers2.get("User-Agent"), "test");
    }

    @Test
    void testCreateTableDefaultOptions() throws Exception {
        String catalogConfigKey = "default-key";
        options.set(TABLE_DEFAULT_OPTION_PREFIX + catalogConfigKey, "default-value");
        RESTCatalog restCatalog = initCatalog(false);
        Identifier identifier = Identifier.create("db1", "new_table_default_options");
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
        restCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, true);
        assertEquals(
                restCatalog.getTable(identifier).options().get(catalogConfigKey), "default-value");
        restCatalog.dropTable(identifier, true);
        restCatalog.dropDatabase(identifier.getDatabaseName(), true, true);

        String catalogConfigInServerKey = "default-key-in-server";
        restCatalog = initCatalogWithDefaultTableOption(catalogConfigInServerKey, "default-value");
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
        restCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, true);
        assertEquals(
                restCatalog.getTable(identifier).options().get(catalogConfigInServerKey),
                "default-value");
    }

    @Test
    void testCatalogManagedPagedPartitionListingDoesNotFallback() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        restCatalogServer.setPartitionListingSupported(false);

        assertThatThrownBy(() -> restCatalog.listPartitionsPaged(identifier, null, null, null))
                .isInstanceOf(NotImplementedException.class);
    }

    @Test
    void testCatalogManagedPartitionListingByNamesDoesNotFallback() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        restCatalogServer.setPartitionListingSupported(false);

        assertThatThrownBy(
                        () ->
                                restCatalog.listPartitionsByNames(
                                        identifier,
                                        Collections.singletonList(
                                                Collections.singletonMap("dt", "20260717"))))
                .isInstanceOf(NotImplementedException.class);
    }

    @Test
    void testCatalogManagedPartitionListingDoesNotFallback() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        restCatalogServer.setPartitionListingSupported(false);

        assertThatThrownBy(() -> restCatalog.listPartitions(identifier))
                .isInstanceOf(NotImplementedException.class);
    }

    @Test
    void testCatalogManagedPartitionListingReflectsCatalogMutationsImmediately() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        FormatTable table = (FormatTable) restCatalog.getTable(identifier);
        FormatTablePartitionManager partitionManager = table.partitionManager();
        assertThat(partitionManager).isNotNull();
        assertThat(partitionManager.listPartitions(Collections.emptyMap(), null)).isEmpty();
        Map<String, String> partition = Collections.singletonMap("dt", "20260717");

        restCatalog.createPartitions(identifier, Collections.singletonList(partition));

        // Listings are not cached, so a mutation through the catalog is visible to the next read.
        assertThat(partitionManager.listPartitions(Collections.emptyMap(), null))
                .extracting(org.apache.paimon.partition.Partition::spec)
                .containsExactly(partition);

        restCatalog.dropPartitions(identifier, Collections.singletonList(partition));

        assertThat(partitionManager.listPartitions(Collections.emptyMap(), null)).isEmpty();
    }

    @Test
    void testCustomPartitionLocationUsesExistingRouteAndStoresCanonicalLocation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        String requested = "OSS://ARCHIVE-BUCKET//history///%64t%3D20260717/";
        String canonical = "oss://archive-bucket/history/dt=20260717";
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), requested);
        options.put("owner", "data-platform");
        String partitionsResource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        restCatalogServer.clearReceivedHeaders();

        restCatalog.createPartitions(
                identifier,
                Collections.singletonList(spec),
                true,
                null,
                false,
                Collections.singletonList(options));

        assertThat(restCatalogServer.getReceivedHeaders(partitionsResource)).hasSize(1);
        assertThat(onlyPartition(identifier).options())
                .containsExactlyInAnyOrderEntriesOf(
                        ImmutableMap.of(
                                CoreOptions.PATH.key(), canonical, "owner", "data-platform"));
    }

    @Test
    void testRenamePreservesCustomPartitionLocation() throws Exception {
        Identifier source = createFormatTableWithCatalogManagedPartitions();
        Identifier destination =
                Identifier.create(source.getDatabaseName(), "renamed_managed_partition_table");
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        String location = "file:/archive/dt=20260717";
        restCatalog.createPartitions(
                source,
                Collections.singletonList(spec),
                true,
                null,
                false,
                partitionOptions(location));

        restCatalog.renameTable(source, destination, false);

        assertThat(restCatalog.listPartitions(destination))
                .singleElement()
                .satisfies(
                        partition -> {
                            assertThat(partition.spec()).isEqualTo(spec);
                            assertThat(customLocation(partition)).isEqualTo(location);
                        });
    }

    @Test
    void testInvalidCustomPartitionLocationFailsBeforePost() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        String partitionsResource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        restCatalogServer.clearReceivedHeaders();

        assertThatThrownBy(
                        () ->
                                restCatalog.createPartitions(
                                        identifier,
                                        Collections.singletonList(spec),
                                        true,
                                        null,
                                        false,
                                        partitionOptions(
                                                "oss://archive-bucket/history/%2e%2e/secret")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid custom partition location");

        assertThat(restCatalogServer.getReceivedHeaders(partitionsResource)).isEmpty();
    }

    @Test
    void testAlignedCustomPartitionLocationsRejectInvalidRequestsBeforeMutation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> first = Collections.singletonMap("dt", "20260717");
        Map<String, String> second = Collections.singletonMap("dt", "20260718");
        String resource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        HttpClient client = new HttpClient(restCatalogServer.getUrl());

        assertThatThrownBy(
                        () ->
                                client.post(
                                        resource,
                                        new RawCreatePartitionsRequest(
                                                Arrays.asList(first, second),
                                                partitionOptions("file:/archive/dt=20260717")),
                                        restCatalog.api().authFunction()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("same size as partitionSpecs");
        assertThatThrownBy(
                        () ->
                                client.post(
                                        resource,
                                        new CreatePartitionsRequest(
                                                Collections.singletonList(first),
                                                true,
                                                null,
                                                null,
                                                partitionOptions("  ")),
                                        restCatalog.api().authFunction()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("Invalid custom partition location");
        assertThatThrownBy(
                        () ->
                                client.post(
                                        resource,
                                        new CreatePartitionsRequest(
                                                Arrays.asList(first, first),
                                                true,
                                                null,
                                                null,
                                                partitionOptions(
                                                        "file:/archive/one", "file:/archive/two")),
                                        restCatalog.api().authFunction()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("must not contain duplicates");
        assertThat(restCatalog.listPartitions(identifier)).isEmpty();
    }

    @Test
    void testPartitionOptionsRejectNullValuesBeforeMutation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        Map<String, String> options = new HashMap<>();
        options.put("owner", null);
        String resource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        HttpClient client = new HttpClient(restCatalogServer.getUrl());

        assertThatThrownBy(
                        () ->
                                client.post(
                                        resource,
                                        new RawCreatePartitionsRequest(
                                                Collections.singletonList(spec),
                                                Collections.singletonList(options)),
                                        restCatalog.api().authFunction()))
                .isInstanceOf(BadRequestException.class)
                .hasMessageContaining("null keys or values");
        assertThat(restCatalog.listPartitions(identifier)).isEmpty();
    }

    @Test
    void testCustomPartitionLocationOwnershipConflictsAreRejectedAtomically() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> first = Collections.singletonMap("dt", "20260717");
        Map<String, String> second = Collections.singletonMap("dt", "20260718");
        List<List<String>> conflicts =
                Arrays.asList(
                        Arrays.asList("file:/archive/shared", "file:/archive/shared"),
                        Arrays.asList("file:/archive/root", "file:/archive/root/nested"),
                        Arrays.asList("file:/archive/root/nested", "file:/archive/root"));

        for (List<String> locations : conflicts) {
            for (boolean ignoreIfExists : Arrays.asList(true, false)) {
                assertThatThrownBy(
                                () ->
                                        restCatalog.createPartitions(
                                                identifier,
                                                Arrays.asList(first, second),
                                                ignoreIfExists,
                                                null,
                                                false,
                                                partitionOptions(locations.toArray(new String[0]))))
                        .isInstanceOf(IllegalArgumentException.class);
                assertThat(restCatalog.listPartitions(identifier)).isEmpty();
            }
        }
    }

    @Test
    void testExistingPartitionRejectsADifferentCustomLocationWithoutMutation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        String originalLocation = "file:/archive/original";
        restCatalog.createPartitions(
                identifier,
                Collections.singletonList(spec),
                true,
                null,
                false,
                partitionOptions(originalLocation));

        assertThatThrownBy(
                        () ->
                                restCatalog
                                        .api()
                                        .createPartitions(
                                                identifier,
                                                Collections.singletonList(spec),
                                                true,
                                                null,
                                                false,
                                                partitionOptions("file:/archive/different")))
                .isInstanceOf(AlreadyExistsException.class)
                .hasMessageContaining("different location");

        assertThat(restCatalog.listPartitions(identifier))
                .extracting(Partition::spec, MockRESTCatalogTest::customLocation)
                .containsExactly(tuple(spec, originalLocation));
    }

    @Test
    void testConcurrentCustomLocationCreatesValidateAndCommitSerially() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> firstSpec = Collections.singletonMap("dt", "20260717");
        Map<String, String> secondSpec = Collections.singletonMap("dt", "20260718");
        String sharedLocation = "file:/archive/shared";
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            Future<?> first =
                    executor.submit(
                            () -> {
                                start.await();
                                restCatalog.createPartitions(
                                        identifier,
                                        Collections.singletonList(firstSpec),
                                        true,
                                        null,
                                        false,
                                        partitionOptions(sharedLocation));
                                return null;
                            });
            Future<?> second =
                    executor.submit(
                            () -> {
                                start.await();
                                restCatalog.createPartitions(
                                        identifier,
                                        Collections.singletonList(secondSpec),
                                        true,
                                        null,
                                        false,
                                        partitionOptions(sharedLocation));
                                return null;
                            });

            start.countDown();
            int failures = 0;
            for (Future<?> future : Arrays.asList(first, second)) {
                try {
                    future.get(10, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    assertThat(e).hasCauseInstanceOf(IllegalArgumentException.class);
                    failures++;
                }
            }
            assertThat(failures).isEqualTo(1);
            List<Partition> stored = restCatalog.listPartitions(identifier);
            assertThat(stored).hasSize(1);
            assertThat(customLocation(stored.get(0))).isEqualTo(sharedLocation);
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testUnsupportedCustomPartitionLocationCreateFailsWithoutMutation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        String resource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        restCatalogServer.setPartitionOptionsCreateSupported(false);
        restCatalogServer.clearReceivedHeaders();

        assertThatThrownBy(
                        () ->
                                restCatalog.createPartitions(
                                        identifier,
                                        Collections.singletonList(spec),
                                        true,
                                        null,
                                        false,
                                        partitionOptions("file:/archive/dt=20260717")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("does not support partition options");

        assertThat(restCatalogServer.getReceivedHeaders(resource)).hasSize(1);
        assertThat(restCatalog.listPartitions(identifier)).isEmpty();
    }

    @Test
    void testEmptyPartitionOptionsDoNotRequireProviderSupport() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        restCatalogServer.setPartitionOptionsCreateSupported(false);

        restCatalog.createPartitions(
                identifier,
                Collections.singletonList(spec),
                true,
                null,
                false,
                Collections.singletonList(Collections.emptyMap()));

        assertThat(onlyPartition(identifier).spec()).isEqualTo(spec);
        assertThat(onlyPartition(identifier).options()).isNull();
    }

    @Test
    void testServerCanonicalizesCustomAndDerivedLocations() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> first = Collections.singletonMap("dt", "20260717");
        Map<String, String> second = Collections.singletonMap("dt", "20260718");
        String resource =
                ResourcePaths.forCatalogProperties(restCatalog.api().options())
                        .partitions(identifier.getDatabaseName(), identifier.getObjectName());
        HttpClient client = new HttpClient(restCatalogServer.getUrl());

        client.post(
                resource,
                new CreatePartitionsRequest(
                        Arrays.asList(first, second),
                        true,
                        null,
                        null,
                        partitionOptions("FILE:///archive//%64t%3D20260717/", null)),
                restCatalog.api().authFunction());

        assertThat(restCatalog.listPartitions(identifier))
                .extracting(Partition::spec, MockRESTCatalogTest::customLocation)
                .containsExactlyInAnyOrder(
                        tuple(first, "file:/archive/dt=20260717"), tuple(second, null));
    }

    @Test
    void testPartitionManagerSurvivesSerialization() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        FormatTable table = (FormatTable) restCatalog.getTable(identifier);
        Map<String, String> partition = Collections.singletonMap("dt", "20260717");
        restCatalog.createPartitions(identifier, Collections.singletonList(partition));

        // A table travels to task processes; its partition catalog must rebuild its client there.
        FormatTablePartitionManager roundTripped =
                InstantiationUtil.clone(table.partitionManager());

        assertThat(roundTripped.listPartitions(Collections.emptyMap(), null))
                .extracting(org.apache.paimon.partition.Partition::spec)
                .containsExactly(partition);
    }

    @Test
    void testReportedPartitionStatisticsAreStoredAndReadBack() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");
        List<Map<String, String>> specs = Collections.singletonList(spec);
        String location = "file:/archive/dt=20260717";
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.PATH.key(), location);
        options.put("owner", "data-platform");
        FormatTablePartitionManager partitionManager =
                ((FormatTable) restCatalog.getTable(identifier)).partitionManager();
        assertThat(partitionManager).isNotNull();

        // A registration on its own measures nothing, so everything starts out unknown.
        restCatalog.createPartitions(
                identifier, specs, true, null, false, Collections.singletonList(options));
        Partition registered = onlyPartition(identifier);
        assertThat(PartitionStatistics.isKnown(registered.recordCount())).isFalse();
        assertThat(PartitionStatistics.isKnown(registered.fileCount())).isFalse();

        // ADD onto a partition nobody measured yet: the report becomes what it holds.
        restCatalog
                .api()
                .createPartitions(
                        identifier,
                        specs,
                        true,
                        Collections.singletonList(
                                new PartitionStatistics(spec, 3L, 300L, 1L, 1000L, -1)),
                        false,
                        null);
        assertStatistics(identifier, 3L, 300L, 1L, 1000L);

        // ADD again, through the partition manager a writer commits with: the counts accumulate
        // and an older file does not move the newest one backwards.
        partitionManager.createPartitions(
                specs,
                true,
                Collections.singletonList(new PartitionStatistics(spec, 4L, 400L, 2L, 500L, -1)),
                false);
        assertStatistics(identifier, 7L, 700L, 3L, 1000L);

        // A field reported as unknown leaves the stored one alone rather than zeroing it.
        restCatalog.createPartitions(
                identifier,
                specs,
                true,
                Collections.singletonList(
                        new PartitionStatistics(
                                spec,
                                PartitionStatistics.UNKNOWN,
                                100L,
                                PartitionStatistics.UNKNOWN,
                                PartitionStatistics.UNKNOWN,
                                -1)),
                false,
                null);
        assertStatistics(identifier, 7L, 800L, 3L, 1000L);

        // SET is the whole partition now: every reported field is replaced, including a creation
        // time that moves backwards because the newer files are gone.
        restCatalog
                .api()
                .createPartitions(
                        identifier,
                        specs,
                        true,
                        Collections.singletonList(
                                new PartitionStatistics(spec, 5L, 500L, 1L, 700L, -1)),
                        true,
                        null);
        assertStatistics(identifier, 5L, 500L, 1L, 700L);

        // Unknown is skipped under SET too: it reports nothing about that field, not a zero.
        restCatalog.createPartitions(
                identifier,
                specs,
                true,
                Collections.singletonList(
                        new PartitionStatistics(
                                spec,
                                PartitionStatistics.UNKNOWN,
                                900L,
                                PartitionStatistics.UNKNOWN,
                                PartitionStatistics.UNKNOWN,
                                -1)),
                true,
                null);
        assertStatistics(identifier, 5L, 900L, 1L, 700L);

        // Reporting never registers or unregisters anything.
        assertThat(restCatalog.listPartitions(identifier)).hasSize(1);
        assertThat(onlyPartition(identifier).options()).isEqualTo(options);
    }

    @Test
    void testStatisticsOfAnUnstoredPartitionAreDropped() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> spec = Collections.singletonMap("dt", "20260717");

        // The statistics describe a partition this request does not register, so the server drops
        // them and keeps the registration.
        restCatalog.createPartitions(
                identifier,
                Collections.singletonList(spec),
                true,
                Collections.singletonList(
                        new PartitionStatistics(
                                Collections.singletonMap("dt", "20260718"),
                                9L,
                                900L,
                                3L,
                                1000L,
                                -1)),
                false,
                null);

        assertThat(restCatalog.listPartitions(identifier))
                .extracting(Partition::spec)
                .containsExactly(spec);
        assertThat(PartitionStatistics.isKnown(onlyPartition(identifier).recordCount())).isFalse();
    }

    @Test
    void testAReportThatOnlyPartlyMatchesIsNotAppliedAtAll() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Map<String, String> stored = Collections.singletonMap("dt", "20260717");
        Map<String, String> absent = Collections.singletonMap("dt", "20260718");
        restCatalog.createPartitions(identifier, Collections.singletonList(stored));

        restCatalog.createPartitions(
                identifier,
                Collections.singletonList(stored),
                true,
                Arrays.asList(
                        new PartitionStatistics(stored, 3L, 300L, 1L, 1000L, -1),
                        new PartitionStatistics(absent, 9L, 900L, 3L, 2000L, -1)),
                false,
                null);

        // Applying the half that matched would count it twice on the next report.
        Partition partition = onlyPartition(identifier);
        assertThat(PartitionStatistics.isKnown(partition.recordCount())).isFalse();
        assertThat(PartitionStatistics.isKnown(partition.fileSizeInBytes())).isFalse();
        assertThat(PartitionStatistics.isKnown(partition.fileCount())).isFalse();
        assertThat(PartitionStatistics.isKnown(partition.lastFileCreationTime())).isFalse();
    }

    private Partition onlyPartition(Identifier identifier) throws Exception {
        List<Partition> partitions = restCatalog.listPartitions(identifier);
        assertThat(partitions).hasSize(1);
        return partitions.get(0);
    }

    private static List<Map<String, String>> partitionOptions(String... locations) {
        List<Map<String, String>> options = new ArrayList<>(locations.length);
        for (String location : locations) {
            options.add(
                    location == null
                            ? Collections.emptyMap()
                            : Collections.singletonMap(CoreOptions.PATH.key(), location));
        }
        return options;
    }

    private static String customLocation(Partition partition) {
        return partition.options() == null ? null : partition.options().get(CoreOptions.PATH.key());
    }

    private void assertStatistics(
            Identifier identifier,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime)
            throws Exception {
        Partition partition = onlyPartition(identifier);
        assertThat(
                        Arrays.asList(
                                partition.recordCount(),
                                partition.fileSizeInBytes(),
                                partition.fileCount(),
                                partition.lastFileCreationTime()))
                .containsExactly(recordCount, fileSizeInBytes, fileCount, lastFileCreationTime);
    }

    @Test
    void testFilteredListingPreservesNextTokenAcrossSparsePage() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        Predicate predicate = partitionFilter("20260717");
        Partition partition =
                new Partition(Collections.singletonMap("dt", "20260717"), 0, 0, 0, 0, -1, false);
        restCatalogServer.enqueueListPartitionsByFilterResponse(null, "p2");
        restCatalogServer.enqueueListPartitionsByFilterResponse(
                Collections.singletonList(partition), null);

        PagedList<Partition> firstPage =
                restCatalog.listPartitionsByFilterPaged(identifier, predicate, 1, null, "dt=2026%");
        assertThat(firstPage.getElements()).isEmpty();
        assertThat(firstPage.getNextPageToken()).isEqualTo("p2");

        PagedList<Partition> secondPage =
                restCatalog.listPartitionsByFilterPaged(
                        identifier, predicate, 1, firstPage.getNextPageToken(), "dt=2026%");
        assertThat(secondPage.getElements()).containsExactly(partition);
        assertThat(secondPage.getNextPageToken()).isNull();

        assertThat(restCatalogServer.getReceivedListPartitionsByFilterRequests())
                .extracting(
                        request ->
                                Arrays.asList(
                                        request.getFilter(),
                                        request.getMaxResults(),
                                        request.getPageToken(),
                                        request.getPartitionNamePattern()))
                .containsExactly(
                        Arrays.asList(JsonSerdeUtil.toFlatJson(predicate), 1, null, "dt=2026%"),
                        Arrays.asList(JsonSerdeUtil.toFlatJson(predicate), 1, "p2", "dt=2026%"));
    }

    @Test
    void testRejectCatalogManagedPartitionsOnExternalTableBeforeCreate() throws Exception {
        Identifier identifier = Identifier.create("db1", "external_partitioned_format_table");
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
        String externalPath = dataPath + "/external-partitioned-format-table";
        Schema schema =
                Schema.newBuilder()
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .option(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .option(CoreOptions.PATH.key(), externalPath)
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build();

        assertThatThrownBy(() -> restCatalog.createTable(identifier, schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("internal table");
        assertThat(restCatalog.listTables(identifier.getDatabaseName()))
                .doesNotContain(identifier.getTableName());
        assertThat(LocalFileIO.create().exists(new Path(externalPath))).isFalse();
    }

    @Test
    void testRoundTrippedFormatTableReplacePassesClientValidation() throws Exception {
        Identifier identifier = createFormatTableWithCatalogManagedPartitions();
        FormatTable existing = (FormatTable) restCatalog.getTable(identifier);
        Schema replacement =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .options(existing.options())
                        .build();
        assertThat(replacement.options())
                .containsEntry(CoreOptions.PATH.key(), existing.location());

        // The mock service does not implement Format Table replacement. Reaching that response
        // proves the REST client accepted the unchanged synthetic path from the loaded table.
        assertThatThrownBy(() -> restCatalog.replaceTable(identifier, replacement, false))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("replaceTable does not support format tables");
    }

    private Identifier createFormatTableWithCatalogManagedPartitions() throws Exception {
        Identifier identifier = Identifier.create("db1", "managed_partition_table");
        restCatalog.createDatabase(identifier.getDatabaseName(), true);
        restCatalog.createTable(
                identifier,
                Schema.newBuilder()
                        .option(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString())
                        .option(CoreOptions.METASTORE_PARTITIONED_TABLE.key(), "true")
                        .option(CoreOptions.FILE_FORMAT.key(), "parquet")
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .partitionKeys("dt")
                        .build(),
                false);
        return identifier;
    }

    private static Predicate partitionFilter(String value) {
        return new PredicateBuilder(
                        RowType.of(
                                new org.apache.paimon.types.DataType[] {DataTypes.STRING()},
                                new String[] {"dt"}))
                .equal(0, value);
    }

    @Test
    void testBaseHeadersInRequests() throws Exception {
        // Set custom headers in options
        String customHeaderName = "custom-header";
        String customHeaderValue = "custom-value";
        options.set(HEADER_PREFIX + customHeaderName, customHeaderValue);

        // Clear any previous headers
        restCatalogServer.clearReceivedHeaders();
        assertEquals(0, restCatalogServer.getReceivedHeaders().size());

        // Initialize catalog with custom headers
        RESTCatalog restCatalog = initCatalog(false);
        // init catalog will trigger REST GetConfig request
        checkHeader(customHeaderName, customHeaderValue);

        // Clear any previous headers
        restCatalogServer.clearReceivedHeaders();
        assertEquals(0, restCatalogServer.getReceivedHeaders().size());

        // Perform an operation that will trigger REST request
        restCatalog.listDatabases();
        checkHeader(customHeaderName, customHeaderValue);
    }

    @Test
    void testReadViaHeaderOnDependencyTableAndDataTokenRequests() throws Exception {
        Identifier root = Identifier.create("db", "root");
        Identifier target = Identifier.create("db", "target");
        RESTCatalog restCatalog = initCatalog(true);
        restCatalog.createDatabase(target.getDatabaseName(), true);
        restCatalog.createTable(target, DEFAULT_TABLE_SCHEMA, false);
        restCatalog.createTable(
                root,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(
                                CoreOptions.BLOB_DESCRIPTOR_SOURCE_TABLE.key(),
                                target.getFullName())
                        .build(),
                false);
        FileStoreTable rootTable = (FileStoreTable) restCatalog.getTable(root);

        restCatalogServer.clearReceivedHeaders();
        BlobDescriptorReaderFactory.create(rootTable);

        String readVia = RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(root));
        ResourcePaths resourcePaths =
                ResourcePaths.forCatalogProperties(restCatalog.api().options());
        assertReadViaHeader(
                resourcePaths.table(target.getDatabaseName(), target.getObjectName()), readVia);
        assertReadViaHeader(
                resourcePaths.tableToken(target.getDatabaseName(), target.getObjectName()),
                readVia);
    }

    @Test
    void testCreateFormatTableWhenEnableDataToken() throws Exception {
        RESTCatalog restCatalog = initCatalog(true);
        restCatalog.createDatabase("test_db", false);
        // Create format table with engine impl without path is not allowed
        Identifier identifier = Identifier.create("test_db", "new_table");
        Schema schema = Schema.newBuilder().column("c1", DataTypes.INT()).build();
        schema.options().put(CoreOptions.TYPE.key(), TableType.FORMAT_TABLE.toString());
        schema.options().put(CoreOptions.FORMAT_TABLE_IMPLEMENTATION.key(), "engine");

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> restCatalog.createTable(identifier, schema, false))
                .withMessage(
                        "Cannot define format-table.implementation is engine for format table when data token is enabled and not define path.");

        // Create format table with engine impl and path
        schema.options().put(CoreOptions.PATH.key(), dataPath + UUID.randomUUID());
        restCatalog.createTable(identifier, schema, false);

        catalog.dropTable(identifier, true);
    }

    @Test
    void testAuthTableQueryResponseWithColumnMasking() throws Exception {
        Identifier identifier = Identifier.create("test_db", "auth_table");
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalog.createTable(
                Identifier.create(identifier.getDatabaseName(), identifier.getTableName()),
                DEFAULT_TABLE_SCHEMA,
                false);

        PredicateBuilder builder =
                new PredicateBuilder(RowType.of(DataTypes.INT(), DataTypes.STRING()));
        Predicate predicate = builder.equal(0, 100);
        String predicateJson = JsonSerdeUtil.toFlatJson(predicate);

        Transform transform =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(1, "col2", DataTypes.STRING())));
        String transformJson = JsonSerdeUtil.toFlatJson(transform);

        // Set up mock response with filter and columnMasking
        List<Predicate> rowFilters = Collections.singletonList(predicate);
        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("col2", transform);
        restCatalogServer.setRowFilterAuth(identifier, rowFilters);
        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);

        TableQueryAuthResult result = catalog.authTableQuery(identifier, null);
        assertThat(result.filter()).containsOnly(predicateJson);
        assertThat(result.columnMasking()).isNotEmpty();
        assertThat(result.columnMasking()).containsKey("col2");
        assertThat(result.columnMasking().get("col2")).isEqualTo(transformJson);

        catalog.dropTable(identifier, true);
        catalog.dropDatabase(identifier.getDatabaseName(), true, true);
    }

    private void checkHeader(String headerName, String headerValue) {
        // Verify that the header were included in the requests
        List<Map<String, String>> receivedHeaders = restCatalogServer.getReceivedHeaders();
        assert receivedHeaders.size() > 0 : "No requests were recorded";

        // Check that request contains our custom headers
        boolean foundCustomHeader = false;

        for (Map<String, String> headers : receivedHeaders) {
            if (headerValue.equals(headers.get(headerName))) {
                foundCustomHeader = true;
            }
        }

        assert foundCustomHeader : "Header was not found in any request";
    }

    private void assertReadViaHeader(String resourcePath, String readVia) {
        assertThat(restCatalogServer.getReceivedHeaders(resourcePath))
                .singleElement()
                .satisfies(
                        headers ->
                                assertThat(headers)
                                        .containsEntry(READ_VIA_HEADER.toLowerCase(), readVia));
    }

    private void testDlfAuth(RESTCatalog restCatalog) throws Exception {
        String databaseName = "db1";
        restCatalog.createDatabase(databaseName, true);
        String[] tableNames = {"dt=20230101", "dt=20230102", "dt=20230103"};
        for (String tableName : tableNames) {
            restCatalog.createTable(
                    Identifier.create(databaseName, tableName), DEFAULT_TABLE_SCHEMA, false);
        }
        PagedList<String> listTablesPaged =
                restCatalog.listTablesPaged(databaseName, 1, "dt=20230101", null, null);
        PagedList<String> listTablesPaged2 =
                restCatalog.listTablesPaged(
                        databaseName, 1, listTablesPaged.getNextPageToken(), null, null);
        assertEquals(listTablesPaged.getElements().get(0), "dt=20230102");
        assertEquals(listTablesPaged2.getElements().get(0), "dt=20230103");
    }

    @Override
    protected Catalog newRestCatalogWithDataToken() throws IOException {
        return initCatalog(true);
    }

    @Override
    protected Catalog newRestCatalogWithDataToken(Map<String, String> extraOptions)
            throws IOException {
        return initCatalog(true, extraOptions);
    }

    @Override
    protected void revokeTablePermission(Identifier identifier) {
        restCatalogServer.addNoPermissionTable(identifier);
    }

    @Override
    protected void revokeViewPermission(Identifier identifier) {
        restCatalogServer.addNoPermissionView(identifier);
    }

    @Override
    protected void authTableColumns(Identifier identifier, List<String> columns) {
        restCatalogServer.addTableColumnAuth(identifier, columns);
    }

    @Override
    protected void revokeDatabasePermission(String database) {
        restCatalogServer.addNoPermissionDatabase(database);
    }

    @Override
    protected RESTToken getDataTokenFromRestServer(Identifier identifier) {
        return restCatalogServer.getDataToken(identifier);
    }

    @Override
    protected void setDataTokenToRestServerForMock(
            Identifier identifier, RESTToken expiredDataToken) {
        restCatalogServer.setDataToken(identifier, expiredDataToken);
    }

    @Override
    protected void resetDataTokenOnRestServer(Identifier identifier) {
        restCatalogServer.removeDataToken(identifier);
    }

    @Override
    protected void updateSnapshotOnRestServer(
            Identifier identifier,
            Snapshot snapshot,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime) {
        restCatalogServer.setTableSnapshot(
                identifier,
                snapshot,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime);
    }

    @Override
    protected void setColumnMasking(Identifier identifier, Map<String, Transform> columnMasking) {
        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);
    }

    @Override
    protected void setRowFilter(Identifier identifier, List<Predicate> rowFilters) {
        restCatalogServer.setRowFilterAuth(identifier, rowFilters);
    }

    private RESTCatalog initCatalog(boolean enableDataToken) throws IOException {
        return initCatalogUtil(enableDataToken, Collections.emptyMap(), null, null);
    }

    private RESTCatalog initCatalog(boolean enableDataToken, Map<String, String> extraOptions)
            throws IOException {
        return initCatalogUtil(enableDataToken, extraOptions, null, null);
    }

    private RESTCatalog initCatalogWithDefaultTableOption(String key, String value)
            throws IOException {
        return initCatalogUtil(false, Collections.emptyMap(), key, value);
    }

    private RESTCatalog initCatalogUtil(
            boolean enableDataToken,
            Map<String, String> extraOptions,
            String createTableDefaultKey,
            String createTableDefaultValue)
            throws IOException {
        String restWarehouse = UUID.randomUUID().toString();
        Map<String, String> defaultConf =
                new HashMap<>(
                        ImmutableMap.of(
                                RESTCatalogInternalOptions.PREFIX.key(),
                                "paimon",
                                "header." + serverDefineHeaderName,
                                serverDefineHeaderValue,
                                RESTTokenFileIO.DATA_TOKEN_ENABLED.key(),
                                enableDataToken + "",
                                CatalogOptions.WAREHOUSE.key(),
                                restWarehouse));
        if (createTableDefaultKey != null) {
            defaultConf.put(
                    TABLE_DEFAULT_OPTION_PREFIX + createTableDefaultKey, createTableDefaultValue);
        }
        this.config = new ConfigResponse(defaultConf, ImmutableMap.of());
        restCatalogServer =
                new RESTCatalogServer(dataPath, this.authProvider, this.config, restWarehouse);
        restCatalogServer.start();
        for (Map.Entry<String, String> entry : this.authMap.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        options.set(CatalogOptions.WAREHOUSE.key(), restWarehouse);
        options.set(RESTCatalogOptions.URI, restCatalogServer.getUrl());
        String path =
                enableDataToken
                        ? dataPath.replaceFirst("file", RESTFileIOTestLoader.SCHEME)
                        : dataPath;
        options.set(RESTTestFileIO.DATA_PATH_CONF_KEY, path);
        for (Map.Entry<String, String> entry : extraOptions.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        return new RESTCatalog(CatalogContext.create(options));
    }

    private static class RawCreatePartitionsRequest implements RESTRequest {

        private final List<Map<String, String>> partitionSpecs;
        private final List<Map<String, String>> partitionOptions;

        private RawCreatePartitionsRequest(
                List<Map<String, String>> partitionSpecs,
                List<Map<String, String>> partitionOptions) {
            this.partitionSpecs = partitionSpecs;
            this.partitionOptions = partitionOptions;
        }

        @JsonGetter("partitionSpecs")
        public List<Map<String, String>> getPartitionSpecs() {
            return partitionSpecs;
        }

        @JsonGetter("partitionOptions")
        public List<Map<String, String>> getPartitionOptions() {
            return partitionOptions;
        }

        @JsonGetter("ignoreIfExists")
        public boolean ignoreIfExists() {
            return true;
        }
    }

    private static class InvalidColumnGrantRequest implements RESTRequest {

        private final PermissionResource resource;

        private InvalidColumnGrantRequest(Identifier identifier) {
            this.resource =
                    new PermissionResource(
                            ResourceType.COLUMN,
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            null,
                            null);
        }

        @JsonGetter("resource")
        public PermissionResource getResource() {
            return resource;
        }

        @JsonGetter("access")
        public String getAccess() {
            return "SELECT";
        }

        @JsonGetter("principal")
        public String getPrincipal() {
            return "analyst";
        }
    }

    private static String extractHost(String uri) {
        String withoutProtocol = uri.replaceFirst("^https?://", "");
        int pathIndex = withoutProtocol.indexOf('/');
        return pathIndex >= 0 ? withoutProtocol.substring(0, pathIndex) : withoutProtocol;
    }

    /**
     * A test-only {@link DLFAuthProvider} variant used on the mock server side. Unlike the
     * production {@link DLFAuthProvider#mergeAuthHeader} which generates a fresh timestamp via
     * {@link java.time.Instant#now()}, this subclass reuses the sign headers already present in the
     * incoming request to recompute the expected authorization. This avoids flaky signature
     * mismatches when the client's signing time and the server's verification time cross a second
     * boundary.
     */
    private static class TestDLFAuthProvider extends DLFAuthProvider {

        private final DLFDefaultSigner signer;
        private final String host;

        TestDLFAuthProvider(DLFToken token, String uri, String region) {
            super(null, token, uri, region, DLFDefaultSigner.IDENTIFIER);
            this.signer = new DLFDefaultSigner(region);
            this.host = extractHost(uri);
        }

        @Override
        public Map<String, String> mergeAuthHeader(
                Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
            try {
                String authorization =
                        signer.authorization(restAuthParameter, token, host, baseHeader);
                Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
                headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
                return headersWithAuth;
            } catch (Exception e) {
                throw new RuntimeException("Failed to verify authorization header", e);
            }
        }
    }
}
