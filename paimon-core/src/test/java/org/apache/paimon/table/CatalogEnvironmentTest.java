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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.SnapshotCommit;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogLoader;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.paimon.options.CatalogOptions.METASTORE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link CatalogEnvironment}. */
class CatalogEnvironmentTest {

    private static final String READ_VIA_OPTION = RESTApi.HEADER_PREFIX + RESTApi.READ_VIA_HEADER;

    @Test
    void testDependencyReadContextForRestCatalog() {
        Identifier root = Identifier.create("db", "root$branch_dev");
        Options options = new Options();
        options.set("other-option", "value");
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = restEnvironment(root, context);

        CatalogContext dependencyContext = environment.dependencyReadContext();

        assertThat(dependencyContext).isNotSameAs(context);
        assertThat(context.options().containsKey(READ_VIA_OPTION)).isFalse();
        assertThat(dependencyContext.options().get(METASTORE))
                .isEqualTo(RESTCatalogFactory.IDENTIFIER);
        assertThat(dependencyContext.options().get("other-option")).isEqualTo("value");
        Identifier readVia =
                JsonSerdeUtil.fromJson(
                        RESTUtil.decodeString(dependencyContext.options().get(READ_VIA_OPTION)),
                        Identifier.class);
        assertThat(readVia).isEqualTo(root);
    }

    @Test
    void testDependencyReadContextPreservesOutermostTable() {
        Identifier outermost = Identifier.create("db", "outermost");
        Options options = new Options();
        options.set(READ_VIA_OPTION, RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(outermost)));
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment =
                restEnvironment(Identifier.create("db", "intermediate"), context);

        assertThat(environment.dependencyReadContext()).isSameAs(context);
        assertThat(context.options().get(READ_VIA_OPTION))
                .isEqualTo(RESTUtil.encodeString(JsonSerdeUtil.toFlatJson(outermost)));
    }

    @Test
    void testDependencyReadContextDoesNotAffectOtherCatalogs() {
        CatalogContext context = CatalogContext.create(new Options());
        CatalogEnvironment environment = environment(Identifier.create("db", "table"), context);

        assertThat(environment.dependencyReadContext()).isSameAs(context);
        assertThat(context.options().containsKey(READ_VIA_OPTION)).isFalse();
    }

    @Test
    void testDependencyReadContextForExternalRestTable() {
        Options options = new Options();
        options.set(METASTORE, RESTCatalogFactory.IDENTIFIER);
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = environment(Identifier.create("db", "external"), context);

        assertThat(environment.dependencyReadContext()).isNotSameAs(context);
    }

    @Test
    void testDependencyReadContextPreservesCustomRestMetastore() {
        Options options = new Options();
        options.set(METASTORE, "custom-rest");
        CatalogContext context = CatalogContext.create(options);
        CatalogEnvironment environment = restEnvironment(Identifier.create("db", "table"), context);

        CatalogContext dependencyContext = environment.dependencyReadContext();

        assertThat(dependencyContext).isNotSameAs(context);
        assertThat(dependencyContext.options().get(METASTORE)).isEqualTo("custom-rest");
    }

    @Test
    void testAppendTableUsesDependencyReadContext() {
        CatalogEnvironment environment = mock(CatalogEnvironment.class);
        when(environment.dependencyReadContext()).thenReturn(CatalogContext.create(new Options()));
        TableSchema schema =
                new TableSchema(
                        0,
                        Collections.singletonList(new DataField(0, "id", DataTypes.INT())),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonMap(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true"),
                        null);
        AppendOnlyFileStoreTable table =
                new AppendOnlyFileStoreTable(
                        mock(FileIO.class), new Path("file:/tmp/table"), schema, environment);

        table.newRead();

        verify(environment).dependencyReadContext();
    }

    @Test
    void testWriterLockUsesCatalogAndRuntimeOptions() throws Exception {
        Catalog catalog = mock(Catalog.class);
        Lock delegate = mock(Lock.class);
        when(delegate.runWithLock(any()))
                .thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(0)).call());
        Options options = new Options();
        options.set(CoreOptions.BRANCH, "dev");
        Identifier branch = new Identifier("db", "table", "dev");
        when(catalog.createLock(branch, "table-id", "writer", options)).thenReturn(delegate);
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("db", "table"),
                        "table-id",
                        () -> catalog,
                        null,
                        null,
                        null,
                        true,
                        false);
        try (Lock lock = environment.createLock(new CoreOptions(options), "writer")) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
            lock.ensureValid();
        }
        verify(catalog).createLock(branch, "table-id", "writer", options);
        verify(delegate).ensureValid();
        verify(delegate).close();
        verify(catalog).close();
    }

    @Test
    void testLegacyFactoryWithoutCatalogLoaderStillWorks() throws Exception {
        CatalogLockFactory factory = mock(CatalogLockFactory.class);
        CatalogLockContext context = CatalogLockContext.fromOptions(new Options());
        CatalogLock backend =
                new CatalogLock() {
                    @Override
                    public <T> T runWithLock(String database, String table, Callable<T> callable)
                            throws Exception {
                        assertThat(database).isEqualTo("db");
                        assertThat(table).isEqualTo("table");
                        return callable.call();
                    }

                    @Override
                    public void close() {}
                };
        when(factory.createLock(context)).thenReturn(backend);
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("db", "table"),
                        null,
                        null,
                        factory,
                        context,
                        null,
                        false,
                        false);
        try (Lock lock = environment.createLock(new CoreOptions(new Options()), "writer")) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
        }
        verify(factory).createLock(context);
    }

    @Test
    void testFailedLockCreationClosesCatalog() throws Exception {
        Catalog catalog = mock(Catalog.class);
        when(catalog.createLock(any(), any(), any(), any()))
                .thenThrow(new IllegalArgumentException("Unsupported"));
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("db", "table"),
                        "table-id",
                        () -> catalog,
                        null,
                        null,
                        null,
                        true,
                        false);
        assertThatThrownBy(() -> environment.createLock(new CoreOptions(new Options()), "writer"))
                .hasMessage("Unsupported");
        verify(catalog).close();
    }

    @Test
    void testDirectSnapshotCommitRetainsLegacyPublicationLock() throws Exception {
        CatalogLockFactory factory = mock(CatalogLockFactory.class);
        AtomicBoolean held = new AtomicBoolean();
        CatalogLock backend =
                new CatalogLock() {
                    @Override
                    public <T> T runWithLock(String database, String table, Callable<T> action)
                            throws Exception {
                        held.set(true);
                        try {
                            return action.call();
                        } finally {
                            held.set(false);
                        }
                    }

                    @Override
                    public void close() {}
                };
        CatalogLockContext context = CatalogLockContext.fromOptions(new Options());
        when(factory.createLock(context)).thenReturn(backend);
        SnapshotManager manager = mock(SnapshotManager.class);
        FileIO fileIO = mock(FileIO.class);
        Path snapshotPath = new Path("file:/table/snapshot/snapshot-1");
        when(manager.fileIO()).thenReturn(fileIO);
        when(manager.branch()).thenReturn("main");
        when(manager.snapshotPath(1L)).thenReturn(snapshotPath);
        when(fileIO.tryToWriteAtomic(snapshotPath, "{}")).thenReturn(true);
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.id()).thenReturn(1L);
        when(snapshot.toJson())
                .thenAnswer(
                        invocation -> {
                            assertThat(held.get()).isTrue();
                            return "{}";
                        });
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        Identifier.create("db", "table"),
                        null,
                        null,
                        factory,
                        context,
                        null,
                        false,
                        false);
        try (SnapshotCommit publisher = environment.snapshotCommit(manager)) {
            assertThat(publisher.commit(null, snapshot, "main", Collections.emptyList())).isTrue();
        }
        assertThat(held.get()).isFalse();
        verify(factory).createLock(context);
    }

    private static CatalogEnvironment environment(
            Identifier identifier, CatalogContext catalogContext) {
        return new CatalogEnvironment(
                identifier, null, null, null, null, catalogContext, false, false);
    }

    private static CatalogEnvironment restEnvironment(
            Identifier identifier, CatalogContext catalogContext) {
        return new CatalogEnvironment(
                identifier,
                null,
                new RESTCatalogLoader(catalogContext),
                null,
                null,
                catalogContext,
                false,
                false);
    }
}
