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

package org.apache.paimon.flink.lakestream;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.DataCatalogTable;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkCatalogOptions;
import org.apache.paimon.flink.FlinkTableFactory;
import org.apache.paimon.flink.SystemCatalogTable;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the lake-stream wiring in {@link FlinkCatalog}. */
public class LakeStreamCatalogTest {

    private static final ObjectPath T1 = new ObjectPath("db1", "t1");
    private static final ObjectPath T1_RT = new ObjectPath("db1", "t1$rt");

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        FakeStoreCatalogFactory.reset();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private Options conf(String storeIdentifier) {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        if (storeIdentifier != null) {
            conf.setString(CatalogOptions.LAKE_STREAM_STORE.key(), storeIdentifier);
        }
        return conf;
    }

    private FlinkCatalog newCatalog(Options conf) {
        CatalogContext context = CatalogContext.create(conf);
        return new FlinkCatalog(
                CatalogFactory.createCatalog(context, getClass().getClassLoader()),
                "test-catalog",
                context.options().get(FlinkCatalogOptions.DEFAULT_DATABASE),
                context.options(),
                new Configuration(),
                getClass().getClassLoader());
    }

    private FlinkCatalog catalogWithStore() {
        Options conf = conf(FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        return newCatalog(conf);
    }

    private void createTable(FlinkCatalog catalog, ObjectPath path, boolean lakeStreamEnabled)
            throws Exception {
        catalog.createDatabase(
                path.getDatabaseName(), new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.PRIMARY_KEY.key(), "id");
        if (lakeStreamEnabled) {
            options.put(CoreOptions.LAKE_STREAM_ENABLED.key(), "true");
        }
        ResolvedSchema resolvedSchema =
                new ResolvedSchema(
                        Collections.singletonList(Column.physical("id", DataTypes.INT().notNull())),
                        Collections.<WatermarkSpec>emptyList(),
                        null);
        CatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.newBuilder()
                                .schema(
                                        Schema.newBuilder()
                                                .fromResolvedSchema(resolvedSchema)
                                                .build())
                                .partitionKeys(Collections.<String>emptyList())
                                .options(options)
                                .build(),
                        resolvedSchema);
        catalog.createTable(path, table, false);
    }

    // ------------------------------------------------------------------------
    //  option
    // ------------------------------------------------------------------------

    @Test
    public void testLakeStreamStoreOptionDeclared() {
        assertThat(CatalogOptions.LAKE_STREAM_STORE.key()).isEqualTo("lakestream.store");
        assertThat(CatalogOptions.LAKE_STREAM_STORE.hasDefaultValue()).isFalse();
    }

    // ------------------------------------------------------------------------
    //  store catalog discovery (PIP-39 L209-250)
    // ------------------------------------------------------------------------

    @Test
    public void testGetOrCreateStoreCatalogExtractsPrefixedOptions() throws Exception {
        Options conf = conf(FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        conf.setString("fakestore.extra", "abc");
        conf.setString("some.unrelated", "xyz");
        FlinkCatalog catalog = newCatalog(conf);

        org.apache.flink.table.catalog.Catalog store = catalog.getOrCreateLakeStreamStoreCatalog();

        assertThat(store).isInstanceOf(FakeStoreCatalog.class);
        assertThat(((FakeStoreCatalog) store).isOpened()).isTrue();
        assertThat(FakeStoreCatalogFactory.createdCatalogOptions()).hasSize(1);
        Map<String, String> passed = FakeStoreCatalogFactory.createdCatalogOptions().get(0);
        assertThat(passed)
                .containsEntry("bootstrap.servers", "localhost:9123")
                .containsEntry("extra", "abc");
        assertThat(passed)
                .doesNotContainKeys(
                        "fakestore.bootstrap.servers",
                        "fakestore.extra",
                        CatalogOptions.LAKE_STREAM_STORE.key(),
                        CatalogOptions.WAREHOUSE.key(),
                        "some.unrelated");
        catalog.close();
    }

    @Test
    public void testStoreCatalogReceivesCatalogConfigurationWhenFirstCreatedViaGetTable()
            throws Exception {
        Options conf = conf(FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");

        // stands in for a platform-injected credential carried by the Flink configuration of the
        // CREATE CATALOG statement rather than by a catalog option
        ConfigOption<String> tokenPath =
                ConfigOptions.key("platform.injected.token.path").stringType().noDefaultValue();
        Configuration flinkConf = new Configuration();
        flinkConf.set(tokenPath, "/var/run/secrets/token");

        FlinkCatalog catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "test-catalog",
                                        conf.toMap(),
                                        flinkConf,
                                        getClass().getClassLoader()));
        try {
            createTable(catalog, T1, true);

            // the planner resolves table metadata before it creates a source, so the store catalog
            // is normally first created through getTable(), which has no table-factory context
            catalog.getTable(T1_RT);

            assertThat(FakeStoreCatalogFactory.createdCatalogConfigurations()).hasSize(1);
            assertThat(FakeStoreCatalogFactory.createdCatalogConfigurations().get(0).get(tokenPath))
                    .isEqualTo("/var/run/secrets/token");
        } finally {
            catalog.close();
        }
    }

    @Test
    public void testStoreDiscoveryUsesTheClassLoaderSuppliedToTheCatalogFactory() throws Exception {
        Options conf = conf(FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        FlinkCatalog catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "test-catalog",
                                        conf.toMap(),
                                        new Configuration(),
                                        getClass().getClassLoader()));

        // a store connector can be visible to the loader Flink handed to the catalog factory while
        // the thread that first touches the store knows nothing about it
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], null));
        try {
            assertThat(catalog.getOrCreateLakeStreamStoreCatalog())
                    .isInstanceOf(FakeStoreCatalog.class);
            assertThat(FakeStoreCatalogFactory.createdCatalogClassLoaders())
                    .containsExactly(getClass().getClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
            catalog.close();
        }
    }

    @Test
    public void testStoreCatalogIsCachedAndReused() throws Exception {
        FlinkCatalog catalog = catalogWithStore();

        org.apache.flink.table.catalog.Catalog first = catalog.getOrCreateLakeStreamStoreCatalog();
        org.apache.flink.table.catalog.Catalog second = catalog.getOrCreateLakeStreamStoreCatalog();

        assertThat(second).isSameAs(first);
        assertThat(FakeStoreCatalogFactory.createdCatalogs()).hasSize(1);
        catalog.close();
    }

    @Test
    public void testMissingStoreOptionFails() throws Exception {
        FlinkCatalog catalog = newCatalog(conf(null));

        assertThatThrownBy(() -> catalog.getOrCreateLakeStreamStoreCatalog())
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(CatalogOptions.LAKE_STREAM_STORE.key());
        catalog.close();
    }

    @Test
    public void testUndiscoverableStoreIdentifierFails() throws Exception {
        FlinkCatalog catalog = newCatalog(conf("nosuchstore"));

        assertThatThrownBy(() -> catalog.getOrCreateLakeStreamStoreCatalog())
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("nosuchstore");
        catalog.close();
    }

    @Test
    public void testMissingRequiredProviderOptionFails() throws Exception {
        // fakestore requires 'bootstrap.servers' (as 'fakestore.bootstrap.servers' in the catalog
        // options); it is deliberately missing here.
        FlinkCatalog catalog = newCatalog(conf(FakeStoreCatalogFactory.IDENTIFIER));

        assertThatThrownBy(() -> catalog.getOrCreateLakeStreamStoreCatalog())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("bootstrap.servers");
        catalog.close();
    }

    @Test
    public void testCloseClosesCachedStoreCatalog() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        catalog.getOrCreateLakeStreamStoreCatalog();

        catalog.close();

        assertThat(FakeStoreCatalogFactory.createdCatalogs()).hasSize(1);
        assertThat(FakeStoreCatalogFactory.createdCatalogs().get(0).isClosed()).isTrue();
    }

    @Test
    public void testCloseWithoutStoreCatalogIsHarmless() throws Exception {
        FlinkCatalog catalog = catalogWithStore();

        assertThatCode(catalog::close).doesNotThrowAnyException();
        assertThat(FakeStoreCatalogFactory.createdCatalogs()).isEmpty();
    }

    // ------------------------------------------------------------------------
    //  $rt interception (PIP-39 L98-116, L254)
    // ------------------------------------------------------------------------

    @Test
    public void testGetTableRealTimeRoutesToStoreWithBaseIdentifier() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        CatalogBaseTable result = catalog.getTable(T1_RT);

        assertThat(result.getComment()).isEqualTo(FakeStoreCatalog.SENTINEL_COMMENT);
        assertThat(FakeStoreCatalog.getTableCalls()).containsExactly(T1);
        catalog.close();
    }

    @Test
    public void testGetTableRealTimeThrowsWhenLakeStreamDisabled() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, false);

        assertThatThrownBy(() -> catalog.getTable(T1_RT))
                .isInstanceOf(TableNotExistException.class);
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testGetTableRealTimeThrowsWhenBaseTableMissing() throws Exception {
        FlinkCatalog catalog = catalogWithStore();

        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("db1", "no_such$rt")))
                .isInstanceOf(TableNotExistException.class);
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testGetTableRealTimeFailsClearlyWhenTheStoreSideIsMissing() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);
        FakeStoreCatalog.markTableMissing(T1);

        // the store's own not-exists error would name the base table and this catalog, which does
        // not tell the user that the store side is what is absent
        assertThatThrownBy(() -> catalog.getTable(T1_RT))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not exist in the Lakestream Store")
                .hasMessageContaining(FakeStoreCatalogFactory.IDENTIFIER);
        catalog.close();
    }

    @Test
    public void testGetTableRealTimeNamesTheMissingStoreOption() throws Exception {
        FlinkCatalog catalog = newCatalog(conf(null));
        createTable(catalog, T1, true);

        // the '$rt' name is served by the store and this catalog names no store, so the
        // failure has to say that; reporting an unknown table would send the user looking for a
        // table that is there
        assertThatThrownBy(() -> catalog.getTable(T1_RT))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining(CatalogOptions.LAKE_STREAM_STORE.key())
                .hasMessageContaining("test-catalog");
        catalog.close();
    }

    @Test
    public void testGetTableRealTimeRejectsTimeTravel() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        assertThatThrownBy(() -> catalog.getTable(T1_RT, 12345L))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("time travel");
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testGetTableTimeTravelDelegatesForNormalTable() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        CatalogBaseTable result = catalog.getTable(T1, 12345L);

        assertThat(result.getOptions())
                .containsEntry(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), "12345");
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testBranchSuffixIsNotTreatedAsRealTime() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        assertThatThrownBy(() -> catalog.getTable(new ObjectPath("db1", "t1$branch_b1$rt")))
                .isInstanceOf(TableNotExistException.class);
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    @Test
    public void testPaimonSystemTableStillDelegates() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        CatalogBaseTable result = catalog.getTable(new ObjectPath("db1", "t1$snapshots"));

        assertThat(result).isInstanceOf(SystemCatalogTable.class);
        assertThat(FakeStoreCatalog.getTableCalls()).isEmpty();
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  recovery from a mis-configured table
    // ------------------------------------------------------------------------

    @Test
    public void testCorrectingAMisconfiguredLakeStreamTableNeverContactsTheStore()
            throws Exception {
        // an identifier that no CatalogFactory answers to, so any attempt to create the store
        // catalog would fail and make this test fail
        FlinkCatalog catalog = newCatalog(conf("nonexistent-store"));
        createTable(catalog, T1, true);

        catalog.dropTable(T1, false);
        createTable(catalog, T1, false);

        assertThat(FakeStoreCatalogFactory.createdCatalogs()).isEmpty();
        assertThat(catalog.tableExists(T1)).isTrue();
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  inherited Paimon catalog behaviour
    // ------------------------------------------------------------------------

    @Test
    public void testInheritedPaimonCatalogBehaviourIsIntact() throws Exception {
        FlinkCatalog catalog = catalogWithStore();
        createTable(catalog, T1, true);

        assertThat(catalog.getName()).isEqualTo("test-catalog");
        assertThat(catalog.getDefaultDatabase()).isEqualTo("default");
        assertThat(catalog.listDatabases()).contains("db1", "default");
        assertThat(catalog.databaseExists("db1")).isTrue();
        assertThat(catalog.getDatabase("db1")).isNotNull();
        assertThat(catalog.listTables("db1")).contains("t1");
        assertThat(catalog.listViews("db1")).isEmpty();
        assertThat(catalog.tableExists(T1)).isTrue();
        assertThat(catalog.tableExists(new ObjectPath("db1", "no_such"))).isFalse();
        assertThat(catalog.getTable(T1)).isInstanceOf(DataCatalogTable.class);
        assertThat(catalog.getTableStatistics(T1)).isNotNull();
        assertThat(catalog.listFunctions("db1")).isEmpty();
        assertThat(catalog.functionExists(new ObjectPath("db1", "f"))).isFalse();

        List<String> procedures = catalog.listProcedures("db1");
        assertThat(procedures).isNotEmpty();
        // paimon procedures are only served from the 'sys' database
        assertThat(catalog.getProcedure(new ObjectPath("sys", procedures.get(0)))).isNotNull();

        assertThat(catalog.getFactory()).isPresent();
        assertThat(catalog.getFactory().get()).isInstanceOf(FlinkTableFactory.class);

        // it is still a Paimon Flink catalog, so the Paimon catalog behind it stays reachable
        assertThat(catalog).isInstanceOf(FlinkCatalog.class);
        assertThat(
                        catalog.catalog()
                                .getTable(
                                        Identifier.create(
                                                T1.getDatabaseName(), T1.getObjectName())))
                .isNotNull();

        catalog.dropTable(T1, false);
        assertThat(catalog.tableExists(T1)).isFalse();
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  table factory plumbing (spec Part 8)
    // ------------------------------------------------------------------------

    @Test
    public void testFlinkTableFactoryConstructorOverloads() throws Exception {
        FlinkCatalog catalog = catalogWithStore();

        assertThat(new FlinkTableFactory().factoryIdentifier())
                .isEqualTo(FlinkCatalogFactory.IDENTIFIER);
        assertThat(new FlinkTableFactory(null, null).factoryIdentifier())
                .isEqualTo(FlinkCatalogFactory.IDENTIFIER);
        assertThat(
                        new FlinkTableFactory(catalog, new LakeStreamTableFactory(catalog))
                                .factoryIdentifier())
                .isEqualTo(FlinkCatalogFactory.IDENTIFIER);
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  FlinkCatalogFactory
    // ------------------------------------------------------------------------

    @Test
    public void testFlinkCatalogFactoryEnablesLakeStreamWhenStoreConfigured() {
        Options conf = conf(FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");

        FlinkCatalog catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "test-catalog",
                                        conf.toMap(),
                                        new Configuration(),
                                        getClass().getClassLoader()));

        assertThat(catalog.lakeStreamStoreConfigured()).isTrue();
        assertThat(catalog.catalog()).isNotNull();
        catalog.close();
    }

    @Test
    public void testFlinkCatalogFactoryLeavesLakeStreamOffWithoutStore() {
        Options conf = conf(null);

        FlinkCatalog catalog =
                new FlinkCatalogFactory()
                        .createCatalog(
                                new FactoryUtil.DefaultCatalogContext(
                                        "test-catalog",
                                        conf.toMap(),
                                        new Configuration(),
                                        getClass().getClassLoader()));

        assertThat(catalog).isInstanceOf(FlinkCatalog.class);
        assertThat(catalog.lakeStreamStoreConfigured()).isFalse();
        catalog.close();
    }
}
