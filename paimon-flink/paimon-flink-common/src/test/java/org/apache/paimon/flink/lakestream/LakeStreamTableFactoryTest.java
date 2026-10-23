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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkCatalogOptions;
import org.apache.paimon.flink.FlinkTableFactory;
import org.apache.paimon.flink.sink.FlinkTableSink;
import org.apache.paimon.flink.source.DataTableSource;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FormatTable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the lake-stream routing in {@link org.apache.paimon.flink.AbstractFlinkTableFactory}
 * (PIP-39 source creation L252-281 and sink creation L283-350), exercised through {@link
 * FlinkTableFactory} created by {@link FlinkCatalog#getFactory()} with the fake store.
 */
public class LakeStreamTableFactoryTest {

    private static final ObjectPath T1 = new ObjectPath("db1", "t1");
    private static final ObjectPath T1_RT = new ObjectPath("db1", "t1$rt");
    private static final ObjectPath T2 = new ObjectPath("db1", "t2");

    @TempDir java.nio.file.Path tempDir;

    @BeforeEach
    public void beforeEach() {
        FakeStoreCatalogFactory.reset();
    }

    // ------------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------------

    private Options conf(String factoryMode) {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        conf.setString(CatalogOptions.LAKE_STREAM_STORE.key(), FakeStoreCatalogFactory.IDENTIFIER);
        conf.setString("fakestore.bootstrap.servers", "localhost:9123");
        if (factoryMode != null) {
            conf.setString("fakestore." + FakeStoreCatalog.FACTORY_MODE_OPTION, factoryMode);
        }
        return conf;
    }

    private FlinkCatalog newCatalog(String factoryMode) {
        return new FlinkCatalogFactory()
                .createCatalog(
                        new FactoryUtil.DefaultCatalogContext(
                                "test-catalog",
                                conf(factoryMode).toMap(),
                                new Configuration(),
                                getClass().getClassLoader()));
    }

    /** A catalog over the same warehouse that was created without {@code lakestream.store}. */
    private FlinkCatalog newCatalogWithoutStore() {
        Options conf = new Options();
        conf.setString(CatalogOptions.WAREHOUSE.key(), tempDir.toString());
        return new FlinkCatalogFactory()
                .createCatalog(
                        new FactoryUtil.DefaultCatalogContext(
                                "test-catalog",
                                conf.toMap(),
                                new Configuration(),
                                getClass().getClassLoader()));
    }

    private static ResolvedSchema nullableIdSchema() {
        return new ResolvedSchema(
                Collections.singletonList(Column.physical("id", DataTypes.INT())),
                Collections.<WatermarkSpec>emptyList(),
                null);
    }

    private static ResolvedSchema idSchema() {
        return new ResolvedSchema(
                Collections.singletonList(Column.physical("id", DataTypes.INT().notNull())),
                Collections.<WatermarkSpec>emptyList(),
                UniqueConstraint.primaryKey("PK_id", Collections.singletonList("id")));
    }

    private void createTable(
            org.apache.flink.table.catalog.Catalog catalog,
            ObjectPath path,
            boolean lakeStreamEnabled)
            throws Exception {
        catalog.createDatabase(
                path.getDatabaseName(), new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        if (lakeStreamEnabled) {
            options.put(CoreOptions.LAKE_STREAM_ENABLED.key(), "true");
        }
        ResolvedSchema resolvedSchema = idSchema();
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

    private static ResolvedCatalogTable resolved(CatalogBaseTable table, ResolvedSchema schema) {
        return new ResolvedCatalogTable((CatalogTable) table, schema);
    }

    /** A {@code null} table is for a decision that is reached before the table is asked for. */
    private DynamicTableFactory.Context context(
            ObjectIdentifier identifier, ResolvedCatalogTable table) {
        return context(identifier, table, new Configuration());
    }

    private DynamicTableFactory.Context context(
            ObjectIdentifier identifier, ResolvedCatalogTable table, ReadableConfig configuration) {
        return new DynamicTableFactory.Context() {
            @Override
            public ObjectIdentifier getObjectIdentifier() {
                return identifier;
            }

            @Override
            public ResolvedCatalogTable getCatalogTable() {
                if (table == null) {
                    throw new UnsupportedOperationException();
                }
                return table;
            }

            @Override
            public boolean isTemporary() {
                return false;
            }

            @Override
            public ReadableConfig getConfiguration() {
                return configuration;
            }

            @Override
            public ClassLoader getClassLoader() {
                return getClass().getClassLoader();
            }
        };
    }

    private static ObjectIdentifier id(String objectName) {
        return ObjectIdentifier.of("test-catalog", "db1", objectName);
    }

    private static DynamicTableSink.Context sinkContext(boolean bounded) {
        return new DynamicTableSink.Context() {
            @Override
            public boolean isBounded() {
                return bounded;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType logicalType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                    DataType dataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<int[][]> getTargetColumns() {
                return Optional.empty();
            }
        };
    }

    // ------------------------------------------------------------------------
    //  source routing (PIP-39 L252-281)
    // ------------------------------------------------------------------------

    @Test
    public void testRealTimeReadRoutesToStoreSourceFactory() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        // the planner resolves 't1$rt' through the wrapper, which returns the store table
        ResolvedCatalogTable storeTable = resolved(catalog.getTable(T1_RT), nullableIdSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$rt"), storeTable));

        assertThat(source).isInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).hasSize(1);
        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sourceContexts().get(0);
        // delegated with the BASE identifier, without the '$rt' suffix
        assertThat(delegated.getObjectIdentifier().getObjectName()).isEqualTo("t1");
        assertThat(delegated.getObjectIdentifier().getDatabaseName()).isEqualTo("db1");
        // the store table's options
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore");
        // the store connection options after prefix removal
        assertThat(delegated.getConfiguration().get(FakeStoreCatalogFactory.BOOTSTRAP_SERVERS))
                .isEqualTo("localhost:9123");
        catalog.close();
    }

    @Test
    public void testQueryHintsReachTheStoreSourceFactory() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        // the planner applies '/*+ OPTIONS(...) */' on top of the table the catalog resolved, and a
        // real store needs those hints to reach its source factory, e.g. to bound a log scan
        CatalogBaseTable storeTable = catalog.getTable(T1_RT);
        Map<String, String> hintedOptions = new HashMap<>(storeTable.getOptions());
        hintedOptions.put("scan.startup.mode", "earliest");
        ResolvedCatalogTable hinted =
                resolved(((CatalogTable) storeTable).copy(hintedOptions), nullableIdSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        factory.createDynamicTableSource(context(id("t1$rt"), hinted));

        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sourceContexts().get(0);
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore")
                .containsEntry("scan.startup.mode", "earliest");
        catalog.close();
    }

    @Test
    public void testOriginalNameReadCreatesPaimonSource() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1"), paimonTable));

        assertThat(source).isInstanceOf(DataTableSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadOnNonLakeStreamTableIsNotRouted() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T2, false);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t2$rt"), paimonTable));

        // the wrapper already rejects 't2$rt' resolution; at factory level the gate must not route
        // a '$rt' identifier whose base table has lakestream.enabled = false
        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadWithBranchIsNotRouted() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$branch_b1$rt"), paimonTable));

        // a branch access must never be misrouted to the store ('endsWith($rt)' would misroute it)
        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadInConnectorModeIsNotRouted() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        // connector mode: the factory is discovered by identifier and gets no catalog at all
        FlinkTableFactory factory = new FlinkTableFactory();

        DynamicTableSource source =
                factory.createDynamicTableSource(context(id("t1$rt"), paimonTable));

        assertThat(source).isNotInstanceOf(FakeStoreSource.class);
        assertThat(FakeStoreTableFactory.sourceContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testRealTimeReadFailsWhenStoreExposesNoSourceFactory() throws Exception {
        FlinkCatalog catalog = newCatalog("sink-only");
        createTable(catalog, T1, true);
        ResolvedCatalogTable storeTable = resolved(catalog.getTable(T1_RT), nullableIdSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSource(context(id("t1$rt"), storeTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }

    // ------------------------------------------------------------------------
    //  sink routing (PIP-39 L283-350)
    // ------------------------------------------------------------------------

    @Test
    public void testRegularWriteGetsTheStoreSink() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        // a regular write is executed by the store, so the planner is handed the store's own sink
        // and discovers its abilities on it directly
        assertThat(sink).isInstanceOf(FakeStoreSink.class);
        assertThat(sink).isNotInstanceOf(FlinkTableSink.class);

        // the store sink is built through the delegated context
        assertThat(FakeStoreTableFactory.sinkContexts()).hasSize(1);
        DynamicTableFactory.Context delegated = FakeStoreTableFactory.sinkContexts().get(0);
        // regular writes keep the ORIGINAL identifier (no '$rt' involved)
        assertThat(delegated.getObjectIdentifier().getObjectName()).isEqualTo("t1");
        assertThat(delegated.getCatalogTable().getOptions())
                .containsEntry("connector", "fakestore");
        assertThat(delegated.getConfiguration().get(FakeStoreCatalogFactory.BOOTSTRAP_SERVERS))
                .isEqualTo("localhost:9123");

        assertThat(sink.asSummaryString()).isEqualTo("FakeStoreSink");
        assertThat(sink.getSinkRuntimeProvider(sinkContext(true)))
                .isInstanceOf(SinkV2Provider.class);
        catalog.close();
    }

    @Test
    public void testRowLevelOperationsAreForwardedToTheStore() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        FakeStoreSink.resetAbilityCalls();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        // a DELETE/UPDATE is a regular write, so the store executes it
        ((SupportsRowLevelDelete) sink).applyRowLevelDelete(null);
        ((SupportsRowLevelUpdate) sink)
                .applyRowLevelUpdate(
                        Collections.singletonList(Column.physical("id", DataTypes.INT())), null);
        ((SupportsDeletePushDown) sink)
                .applyDeleteFilters(Collections.<ResolvedExpression>emptyList());
        ((SupportsDeletePushDown) sink).executeDeletion();

        assertThat(FakeStoreSink.abilityCalls())
                .contains(
                        "applyRowLevelDelete",
                        "applyRowLevelUpdate(1)",
                        "applyDeleteFilters(0)",
                        "executeDeletion");
        catalog.close();
    }

    @Test
    public void testStaticPartitionReachesTheStoreSink() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        FakeStoreSink.resetAbilityCalls();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t1"), paimonTable));

        Map<String, String> partition = new HashMap<>();
        partition.put("dt", "20261019");

        // a regular write is executed by the store, so the partition spec is applied to its sink
        ((SupportsPartitioning) sink).applyStaticPartition(partition);
        assertThat(FakeStoreSink.abilityCalls()).contains("applyStaticPartition({dt=20261019})");
        catalog.close();
    }

    @Test
    public void testFormatTableCannotBeALakeStreamTable() throws Exception {
        // a format table has no Paimon history for the store to tier into, so it is rejected on
        // both paths rather than left half routed
        FormatTable formatTable = mock(FormatTable.class);
        when(formatTable.options())
                .thenReturn(
                        Collections.singletonMap(CoreOptions.LAKE_STREAM_ENABLED.key(), "true"));
        Catalog paimonCatalog = mock(Catalog.class);
        when(paimonCatalog.getTable(any(Identifier.class))).thenReturn(formatTable);

        Options options = new Options();
        options.set(FlinkCatalogOptions.DISABLE_CREATE_TABLE_IN_DEFAULT_DB, true);
        FlinkCatalog catalog = new FlinkCatalog(paimonCatalog, "test-catalog", "default", options);
        LakeStreamTableFactory lakeStream = new LakeStreamTableFactory(catalog);

        assertThatThrownBy(() -> lakeStream.createDynamicTableSink(context(id("t1"), null)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("format tables")
                .hasMessageContaining(CoreOptions.LAKE_STREAM_ENABLED.key());
        assertThatThrownBy(() -> lakeStream.createDynamicTableSource(context(id("t1$rt"), null)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("format tables");
        catalog.close();
    }

    @Test
    public void testSinkNotRoutedWhenLakeStreamDisabled() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T2, false);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t2"), paimonTable));

        assertThat(sink).isInstanceOf(FlinkTableSink.class);
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testJobLevelOptionCannotMakeTheGuardBlameAMissingStore() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T2, false);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        // a job-level dynamic option reaches the built table but not the persisted one, so the
        // write stays on Paimon. The missing-store guard must stay quiet: this catalog has a store,
        // and blaming its absence would be a false diagnosis.
        Configuration configuration = new Configuration();
        configuration.setString(
                "paimon.test-catalog.db1.t2." + CoreOptions.LAKE_STREAM_ENABLED.key(), "true");

        DynamicTableSink sink =
                factory.createDynamicTableSink(context(id("t2"), paimonTable, configuration));

        assertThat(sink).isInstanceOf(FlinkTableSink.class);
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testConnectorModeCannotBypassTheStore() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        // connector mode: the factory is discovered by identifier and gets no catalog at all, so it
        // has no store to route to
        FlinkTableFactory factory = new FlinkTableFactory();

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(CatalogOptions.LAKE_STREAM_STORE.key())
                .hasMessageContaining(CoreOptions.LAKE_STREAM_ENABLED.key());
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        catalog.close();
    }

    @Test
    public void testWriteToLakeStreamTableWithoutStoreConfiguredIsRejected() throws Exception {
        FlinkCatalog withStore = newCatalog(null);
        createTable(withStore, T1, true);
        withStore.close();

        // the same warehouse reached through a catalog that was created without 'lakestream.store'
        FlinkCatalog withoutStore = newCatalogWithoutStore();
        ResolvedCatalogTable paimonTable = resolved(withoutStore.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) withoutStore.getFactory().get();

        // writing to Paimon here would leave the store serving stale-plus-diverged data for the
        // same keys, so the missing catalog option is reported instead
        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(CatalogOptions.LAKE_STREAM_STORE.key())
                .hasMessageContaining("test-catalog.db1.t1");
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        withoutStore.close();
    }

    @Test
    public void testWriteToPlainTableWithoutStoreConfiguredIsAllowed() throws Exception {
        FlinkCatalog withoutStore = newCatalogWithoutStore();
        createTable(withoutStore, T2, false);
        ResolvedCatalogTable paimonTable = resolved(withoutStore.getTable(T2), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) withoutStore.getFactory().get();

        DynamicTableSink sink = factory.createDynamicTableSink(context(id("t2"), paimonTable));

        assertThat(sink).isInstanceOf(FlinkTableSink.class);
        assertThat(FakeStoreTableFactory.sinkContexts()).isEmpty();
        withoutStore.close();
    }

    @Test
    public void testSinkFailsWhenStoreExposesNoSinkFactory() throws Exception {
        FlinkCatalog catalog = newCatalog("source-only");
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }

    @Test
    public void testSinkFailsClearlyWhenTheStoreSideIsMissing() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        FakeStoreCatalog.markTableMissing(T1);
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("does not exist in the Lakestream Store")
                .hasMessageContaining(FakeStoreCatalogFactory.IDENTIFIER);
        catalog.close();
    }

    @Test
    public void testWriteThroughTheRealTimeNameIsRejected() throws Exception {
        FlinkCatalog catalog = newCatalog(null);
        createTable(catalog, T1, true);
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();
        ResolvedCatalogTable storeTable = resolved(catalog.getTable(T1_RT), idSchema());

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1$rt"), storeTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("read-only view")
                .hasMessageContaining("test-catalog.db1.t1");
        catalog.close();
    }

    @Test
    public void testSinkFailsWhenStoreExposesNoFactoryAtAll() throws Exception {
        FlinkCatalog catalog = newCatalog("none");
        createTable(catalog, T1, true);
        ResolvedCatalogTable paimonTable = resolved(catalog.getTable(T1), idSchema());
        FlinkTableFactory factory = (FlinkTableFactory) catalog.getFactory().get();

        assertThatThrownBy(() -> factory.createDynamicTableSink(context(id("t1"), paimonTable)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("fakestore");
        catalog.close();
    }
}
