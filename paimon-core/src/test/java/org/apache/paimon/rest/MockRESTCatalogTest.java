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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.CastTransform;
import org.apache.paimon.predicate.ConcatTransform;
import org.apache.paimon.predicate.ConcatWsTransform;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FieldTransform;
import org.apache.paimon.predicate.GreaterOrEqual;
import org.apache.paimon.predicate.GreaterThan;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.Transform;
import org.apache.paimon.predicate.UpperTransform;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.rest.auth.BearTokenAuthProvider;
import org.apache.paimon.rest.auth.DLFAuthProvider;
import org.apache.paimon.rest.auth.DLFTokenLoader;
import org.apache.paimon.rest.auth.DLFTokenLoaderFactory;
import org.apache.paimon.rest.auth.RESTAuthParameter;
import org.apache.paimon.rest.exceptions.NotAuthorizedException;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.CoreOptions.QUERY_AUTH_ENABLED;
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
        String region = "cn-hangzhou";
        this.authProvider = DLFAuthProvider.fromAccessKey(akId, akSecret, securityToken, region);
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
        String region = "cn-hangzhou";
        String tokenPath = dataPath + UUID.randomUUID();
        generateTokenAndWriteToFile(tokenPath);
        DLFTokenLoader tokenLoader =
                DLFTokenLoaderFactory.createDLFTokenLoader(
                        "local_file",
                        new Options(
                                ImmutableMap.of(
                                        RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath)));
        this.authProvider = DLFAuthProvider.fromTokenLoader(tokenLoader, region);
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

        // Set up mock response with filter and columnMasking
        List<Predicate> rowFilters = Collections.singletonList(predicate);
        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("col2", transform);
        restCatalogServer.setRowFilterAuth(identifier, rowFilters);
        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);

        TableQueryAuthResult result = catalog.authTableQuery(identifier, null);
        assertThat(result.rowFilter()).isEqualTo(predicate);
        assertThat(result.columnMasking()).isNotEmpty();
        assertThat(result.columnMasking()).containsKey("col2");
        assertThat(result.columnMasking().get("col2")).isEqualTo(transform);

        catalog.dropTable(identifier, true);
        catalog.dropDatabase(identifier.getDatabaseName(), true, true);
    }

    @Test
    void testColumnMaskingApplyOnRead() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "auth_table_masking_apply");
        catalog.createDatabase(identifier.getDatabaseName(), true);

        // Create table with multiple columns of different types
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "col1", DataTypes.STRING()));
        fields.add(new DataField(1, "col2", DataTypes.STRING()));
        fields.add(new DataField(2, "col3", DataTypes.INT()));
        fields.add(new DataField(3, "col4", DataTypes.STRING()));
        fields.add(new DataField(4, "col5", DataTypes.STRING()));

        catalog.createTable(
                identifier,
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(QUERY_AUTH_ENABLED.key(), "true"),
                        ""),
                true);

        Table table = catalog.getTable(identifier);

        // Write test data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(
                GenericRow.of(
                        BinaryString.fromString("hello"),
                        BinaryString.fromString("world"),
                        100,
                        BinaryString.fromString("test"),
                        BinaryString.fromString("data")));
        write.write(
                GenericRow.of(
                        BinaryString.fromString("foo"),
                        BinaryString.fromString("bar"),
                        200,
                        BinaryString.fromString("example"),
                        BinaryString.fromString("value")));
        List<CommitMessage> messages = write.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        write.close();
        commit.close();

        // Set up column masking with various transform types
        Map<String, Transform> columnMasking = new HashMap<>();

        // Test 1: ConcatTransform - mask col1 with "****"
        ConcatTransform concatTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("****")));
        columnMasking.put("col1", concatTransform);

        // Test 2: UpperTransform - convert col2 to uppercase
        UpperTransform upperTransform =
                new UpperTransform(
                        Collections.singletonList(new FieldRef(1, "col2", DataTypes.STRING())));
        columnMasking.put("col2", upperTransform);

        // Test 3: CastTransform - cast col3 (INT) to STRING
        CastTransform castTransform =
                new CastTransform(new FieldRef(2, "col3", DataTypes.INT()), DataTypes.STRING());
        columnMasking.put("col3", castTransform);

        // Test 4: ConcatWsTransform - concatenate col4 with separator
        ConcatWsTransform concatWsTransform =
                new ConcatWsTransform(
                        java.util.Arrays.asList(
                                BinaryString.fromString("-"),
                                BinaryString.fromString("prefix"),
                                new FieldRef(3, "col4", DataTypes.STRING())));
        columnMasking.put("col4", concatWsTransform);

        // col5 is intentionally not masked to verify unmasked columns work correctly

        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);

        // Read and verify masked data
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);

        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);

        assertThat(rows).hasSize(2);

        // Verify first row
        InternalRow row1 = rows.get(0);
        assertThat(row1.getString(0).toString())
                .isEqualTo("****"); // col1 masked with ConcatTransform
        assertThat(row1.getString(1).toString())
                .isEqualTo("WORLD"); // col2 masked with UpperTransform
        assertThat(row1.getString(2).toString())
                .isEqualTo("100"); // col3 masked with CastTransform (INT->STRING)
        assertThat(row1.getString(3).toString())
                .isEqualTo("prefix-test"); // col4 masked with ConcatWsTransform
        assertThat(row1.getString(4).toString())
                .isEqualTo("data"); // col5 NOT masked - original value

        // Verify second row
        InternalRow row2 = rows.get(1);
        assertThat(row2.getString(0).toString())
                .isEqualTo("****"); // col1 masked with ConcatTransform
        assertThat(row2.getString(1).toString())
                .isEqualTo("BAR"); // col2 masked with UpperTransform
        assertThat(row2.getString(2).toString())
                .isEqualTo("200"); // col3 masked with CastTransform (INT->STRING)
        assertThat(row2.getString(3).toString())
                .isEqualTo("prefix-example"); // col4 masked with ConcatWsTransform
        assertThat(row2.getString(4).toString())
                .isEqualTo("value"); // col5 NOT masked - original value
    }

    @Test
    void testRowFilter() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "auth_table_filter");
        catalog.createDatabase(identifier.getDatabaseName(), true);

        // Create table with multiple data types
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "id", DataTypes.INT()));
        fields.add(new DataField(1, "name", DataTypes.STRING()));
        fields.add(new DataField(2, "age", DataTypes.BIGINT()));
        fields.add(new DataField(3, "salary", DataTypes.DOUBLE()));
        fields.add(new DataField(4, "is_active", DataTypes.BOOLEAN()));
        fields.add(new DataField(5, "score", DataTypes.FLOAT()));

        catalog.createTable(
                identifier,
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(QUERY_AUTH_ENABLED.key(), "true"),
                        ""),
                true);

        Table table = catalog.getTable(identifier);

        // Write test data with various types
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(GenericRow.of(1, BinaryString.fromString("Alice"), 25L, 50000.0, true, 85.5f));
        write.write(GenericRow.of(2, BinaryString.fromString("Bob"), 30L, 60000.0, false, 90.0f));
        write.write(
                GenericRow.of(3, BinaryString.fromString("Charlie"), 35L, 70000.0, true, 95.5f));
        write.write(GenericRow.of(4, BinaryString.fromString("David"), 28L, 55000.0, true, 88.0f));
        List<CommitMessage> messages = write.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        write.close();
        commit.close();

        // Test 1: Filter by INT type (id > 2)
        LeafPredicate intFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(0, "id", DataTypes.INT())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(2));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(intFilterPredicate));

        List<String> result1 = batchRead(table);
        assertThat(result1).hasSize(2);
        assertThat(result1)
                .contains(
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]",
                        "+I[4, David, 28, 55000.0, true, 88.0]");

        // Test 2: Filter by BIGINT type (age >= 30)
        LeafPredicate bigintFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.BIGINT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30L));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(bigintFilterPredicate));

        List<String> result2 = batchRead(table);
        assertThat(result2).hasSize(2);
        assertThat(result2)
                .contains(
                        "+I[2, Bob, 30, 60000.0, false, 90.0]",
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]");

        // Test 3: Filter by DOUBLE type (salary > 55000.0)
        LeafPredicate doubleFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "salary", DataTypes.DOUBLE())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(55000.0));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(doubleFilterPredicate));

        List<String> result3 = batchRead(table);
        assertThat(result3).hasSize(2);
        assertThat(result3)
                .contains(
                        "+I[2, Bob, 30, 60000.0, false, 90.0]",
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]");

        // Test 4: Filter by BOOLEAN type (is_active = true)
        LeafPredicate booleanFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(4, "is_active", DataTypes.BOOLEAN())),
                        Equal.INSTANCE,
                        Collections.singletonList(true));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(booleanFilterPredicate));

        List<String> result4 = batchRead(table);
        assertThat(result4).hasSize(3);
        assertThat(result4)
                .contains(
                        "+I[1, Alice, 25, 50000.0, true, 85.5]",
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]",
                        "+I[4, David, 28, 55000.0, true, 88.0]");

        // Test 5: Filter by FLOAT type (score >= 90.0)
        LeafPredicate floatFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(5, "score", DataTypes.FLOAT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(90.0f));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(floatFilterPredicate));

        List<String> result5 = batchRead(table);
        assertThat(result5).hasSize(2);
        assertThat(result5)
                .contains(
                        "+I[2, Bob, 30, 60000.0, false, 90.0]",
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]");

        // Test 6: Filter by STRING type (name = "Alice")
        LeafPredicate stringFilterPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(1, "name", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("Alice")));
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(stringFilterPredicate));

        List<String> result6 = batchRead(table);
        assertThat(result6).hasSize(1);
        assertThat(result6).contains("+I[1, Alice, 25, 50000.0, true, 85.5]");

        // Test 7: Filter with two predicates (age >= 30 AND is_active = true)
        LeafPredicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(2, "age", DataTypes.BIGINT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30L));
        LeafPredicate isActiveTruePredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(4, "is_active", DataTypes.BOOLEAN())),
                        Equal.INSTANCE,
                        Collections.singletonList(true));
        restCatalogServer.setRowFilterAuth(
                identifier, Arrays.asList(ageGe30Predicate, isActiveTruePredicate));

        List<String> result7 = batchRead(table);
        assertThat(result7).hasSize(1);
        assertThat(result7).contains("+I[3, Charlie, 35, 70000.0, true, 95.5]");

        // Test 8: Filter with two predicates (salary > 55000.0 AND score >= 90.0)
        LeafPredicate salaryGt55000Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "salary", DataTypes.DOUBLE())),
                        GreaterThan.INSTANCE,
                        Collections.singletonList(55000.0));
        LeafPredicate scoreGe90Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(5, "score", DataTypes.FLOAT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(90.0f));
        restCatalogServer.setRowFilterAuth(
                identifier, Arrays.asList(salaryGt55000Predicate, scoreGe90Predicate));

        List<String> result8 = batchRead(table);
        assertThat(result8).hasSize(2);
        assertThat(result8)
                .contains(
                        "+I[2, Bob, 30, 60000.0, false, 90.0]",
                        "+I[3, Charlie, 35, 70000.0, true, 95.5]");
    }

    @Test
    void testColumnMaskingAndRowFilter() throws Exception {
        Identifier identifier = Identifier.create("test_table_db", "combined_auth_table");
        catalog.createDatabase(identifier.getDatabaseName(), true);

        // Create table with test data
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "id", DataTypes.INT()));
        fields.add(new DataField(1, "name", DataTypes.STRING()));
        fields.add(new DataField(2, "salary", DataTypes.STRING()));
        fields.add(new DataField(3, "age", DataTypes.INT()));
        fields.add(new DataField(4, "department", DataTypes.STRING()));

        catalog.createTable(
                identifier,
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(QUERY_AUTH_ENABLED.key(), "true"),
                        ""),
                true);

        Table table = catalog.getTable(identifier);

        // Write test data
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        BatchTableWrite write = writeBuilder.newWrite();
        write.write(
                GenericRow.of(
                        1,
                        BinaryString.fromString("Alice"),
                        BinaryString.fromString("50000.0"),
                        25,
                        BinaryString.fromString("IT")));
        write.write(
                GenericRow.of(
                        2,
                        BinaryString.fromString("Bob"),
                        BinaryString.fromString("60000.0"),
                        30,
                        BinaryString.fromString("HR")));
        write.write(
                GenericRow.of(
                        3,
                        BinaryString.fromString("Charlie"),
                        BinaryString.fromString("70000.0"),
                        35,
                        BinaryString.fromString("IT")));
        write.write(
                GenericRow.of(
                        4,
                        BinaryString.fromString("David"),
                        BinaryString.fromString("55000.0"),
                        28,
                        BinaryString.fromString("Finance")));
        List<CommitMessage> messages = write.prepareCommit();
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);
        write.close();
        commit.close();

        // Test column masking only
        Transform salaryMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        Map<String, Transform> columnMasking = new HashMap<>();
        columnMasking.put("salary", salaryMaskTransform);
        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);

        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);

        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("***");

        // Test row filter only (clear column masking first)
        restCatalogServer.setColumnMaskingAuth(identifier, new HashMap<>());
        Predicate ageGe30Predicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(3, "age", DataTypes.INT())),
                        GreaterOrEqual.INSTANCE,
                        Collections.singletonList(30));
        restCatalogServer.setRowFilterAuth(identifier, Collections.singletonList(ageGe30Predicate));

        readBuilder = table.newReadBuilder();
        splits = readBuilder.newScan().plan().splits();
        read = readBuilder.newRead();
        reader = read.createReader(splits);

        rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows).hasSize(2);

        // Test both column masking and row filter together
        columnMasking.put("salary", salaryMaskTransform);
        Transform nameMaskTransform =
                new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")));
        columnMasking.put("name", nameMaskTransform);
        restCatalogServer.setColumnMaskingAuth(identifier, columnMasking);
        Predicate deptPredicate =
                LeafPredicate.of(
                        new FieldTransform(new FieldRef(4, "department", DataTypes.STRING())),
                        Equal.INSTANCE,
                        Collections.singletonList(BinaryString.fromString("IT")));
        restCatalogServer.setRowFilterAuth(identifier, Collections.singletonList(deptPredicate));

        readBuilder = table.newReadBuilder();
        splits = readBuilder.newScan().plan().splits();
        read = readBuilder.newRead();
        reader = read.createReader(splits);

        rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("***"); // name masked
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("***"); // salary masked
        assertThat(rows.get(0).getString(4).toString()).isEqualTo("IT"); // department not masked

        // Test complex scenario: row filter + column masking combined
        Predicate combinedPredicate = PredicateBuilder.and(ageGe30Predicate, deptPredicate);
        restCatalogServer.setRowFilterAuth(
                identifier, Collections.singletonList(combinedPredicate));

        readBuilder = table.newReadBuilder();
        splits = readBuilder.newScan().plan().splits();
        read = readBuilder.newRead();
        reader = read.createReader(splits);

        rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getInt(0)).isEqualTo(3); // id
        assertThat(rows.get(0).getString(1).toString()).isEqualTo("***"); // name masked
        assertThat(rows.get(0).getString(2).toString()).isEqualTo("***"); // salary masked
        assertThat(rows.get(0).getInt(3)).isEqualTo(35); // age not masked

        // Clear both column masking and row filter
        restCatalogServer.setColumnMaskingAuth(identifier, new HashMap<>());
        restCatalogServer.setRowFilterAuth(identifier, null);

        readBuilder = table.newReadBuilder();
        splits = readBuilder.newScan().plan().splits();
        read = readBuilder.newRead();
        reader = read.createReader(splits);

        rows = new ArrayList<>();
        reader.forEachRemaining(rows::add);
        assertThat(rows).hasSize(4);
        assertThat(rows.get(0).getString(1).toString()).isIn("Alice", "Bob", "Charlie", "David");
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
}
