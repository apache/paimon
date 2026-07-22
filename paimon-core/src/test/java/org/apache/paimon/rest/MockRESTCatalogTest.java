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
import org.apache.paimon.catalog.ReadAuthorizationContext;
import org.apache.paimon.catalog.ReadAuthorizationResource;
import org.apache.paimon.catalog.ReadAuthorizationRootType;
import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
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
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.requests.CreateReadGrantRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.format.FormatTablePartitionManager;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.catalog.Catalog.TABLE_DEFAULT_OPTION_PREFIX;
import static org.apache.paimon.rest.RESTApi.HEADER_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        String tokenPath = dataPath + UUID.randomUUID();
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
                        RESTCatalogOptions.TOKEN_PROVIDER.key(), AuthProviderEnum.DLF.identifier(),
                        RESTCatalogOptions.DLF_REGION.key(), region,
                        RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath);
        RESTCatalog restCatalog = initCatalog(false);
        testDlfAuth(restCatalog);
        File file = new File(tokenPath);
        file.delete();
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
    void testViewDependenciesRoundTrip() throws Exception {
        Identifier viewIdentifier = Identifier.create("view_dependencies_db", "my_view");
        Identifier initialDependency =
                Identifier.create(viewIdentifier.getDatabaseName(), "initial_table");
        Identifier updatedDependency =
                Identifier.create(viewIdentifier.getDatabaseName(), "updated_table");
        catalog.createDatabase(viewIdentifier.getDatabaseName(), false);

        View baseView = createView(viewIdentifier);
        View view =
                new ViewImpl(
                        viewIdentifier,
                        baseView.rowType().getFields(),
                        baseView.query(),
                        baseView.dialects(),
                        baseView.comment().orElse(null),
                        baseView.options(),
                        Collections.singletonList(initialDependency));
        catalog.createView(viewIdentifier, view, false);

        assertThat(catalog.getView(viewIdentifier).dependencies())
                .containsExactly(initialDependency);
        List<View> listedViews =
                catalog.listViewDetailsPaged(viewIdentifier.getDatabaseName(), null, null, null)
                        .getElements();
        assertThat(listedViews).hasSize(1);
        assertThat(listedViews.get(0).dependencies()).containsExactly(initialDependency);

        catalog.alterView(
                viewIdentifier,
                Collections.singletonList(
                        ViewChange.updateDependencies(
                                Collections.singletonList(updatedDependency))),
                false);

        assertThat(catalog.getView(viewIdentifier).dependencies())
                .containsExactly(updatedDependency);
        listedViews =
                catalog.listViewDetailsPaged(viewIdentifier.getDatabaseName(), null, null, null)
                        .getElements();
        assertThat(listedViews).hasSize(1);
        assertThat(listedViews.get(0).dependencies()).containsExactly(updatedDependency);
    }

    @Test
    void testReadGrantEndpointAndGrantBackedTableRead() throws Exception {
        String database = "read_grant_db";
        Identifier root = Identifier.create(database, "root_view");
        Identifier dependency = Identifier.create(database, "dependency_table");
        catalog.createDatabase(database, false);
        catalog.createTable(dependency, DEFAULT_TABLE_SCHEMA, false);

        View baseView = createView(root);
        catalog.createView(
                root,
                new ViewImpl(
                        root,
                        baseView.rowType().getFields(),
                        baseView.query(),
                        baseView.dialects(),
                        baseView.comment().orElse(null),
                        baseView.options(),
                        Collections.singletonList(dependency)),
                false);

        restCatalogServer.clearReceivedHeaders();
        restCatalogServer.clearReceivedReadGrantRequests();
        assertThat(catalog.getView(root).dependencies()).containsExactly(dependency);
        assertThat(restCatalogServer.getReceivedReadGrantRequests()).isEmpty();

        ReadAuthorizationContext readContext = ReadAuthorizationContext.forView(root);
        catalog.getTable(dependency, readContext);

        List<CreateReadGrantRequest> requests = restCatalogServer.getReceivedReadGrantRequests();
        assertThat(requests).hasSize(1);
        CreateReadGrantRequest request = requests.get(0);
        assertThat(request.rootType()).isEqualTo(ReadAuthorizationRootType.VIEW);
        assertThat(request.rootIdentifier()).isEqualTo(root);
        assertThat(request.targets()).containsExactly(ReadAuthorizationResource.table(dependency));
        assertThat(request.previousReadGrant()).isNull();

        String readGrant =
                readContext
                        .readGrant()
                        .orElseThrow(() -> new AssertionError("No read grant was installed"));
        assertThat(readContext.authorizedResources())
                .containsExactly(ReadAuthorizationResource.table(dependency));
        assertThat(restCatalogServer.getReceivedHeaders())
                .extracting(headers -> headers.get(RESTApi.READ_GRANT_HEADER.toLowerCase()))
                .contains(readGrant);
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
