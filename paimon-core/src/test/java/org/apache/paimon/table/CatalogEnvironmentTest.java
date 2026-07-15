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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.RESTReadVia;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SnapshotLoader;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link CatalogEnvironment}. */
public class CatalogEnvironmentTest {

    @Test
    public void testKeepRealIdentifierAndReadViaSeparately() {
        Identifier table = Identifier.create("db_table", "my_table");
        Identifier view = Identifier.create("db_view", "my_view");

        CatalogEnvironment environment =
                new CatalogEnvironment(table, view, null, null, null, null, null, false, false);

        assertThat(environment.identifier()).isEqualTo(table);
        assertThat(environment.readVia()).contains(view);
        assertThat(environment.readIdentifier()).isEqualTo(RESTReadVia.withReadVia(table, view));
        assertThat(environment.readRoot()).isEqualTo(view);
    }

    @Test
    public void testReadOperationsUseReadIdentifier() throws Exception {
        Identifier table = Identifier.create("db_table", "my_table");
        Identifier view = Identifier.create("db_view", "my_view");
        Identifier readIdentifier = RESTReadVia.withReadVia(table, view);
        Catalog catalog = mock(Catalog.class);
        when(catalog.loadSnapshot(readIdentifier)).thenReturn(Optional.empty());

        CatalogEnvironment environment =
                new CatalogEnvironment(
                        table, view, null, () -> catalog, null, null, null, true, false);

        SnapshotLoader snapshotLoader = environment.snapshotLoader();
        assertThat(snapshotLoader).isNotNull();
        snapshotLoader.load();
        environment
                .tableQueryAuth(
                        CoreOptions.fromMap(
                                Collections.singletonMap(
                                        CoreOptions.QUERY_AUTH_ENABLED.key(), "true")))
                .auth(null);
        Instant instant = Instant.snapshot(1L);
        snapshotLoader.rollback(instant);

        verify(catalog).loadSnapshot(readIdentifier);
        verify(catalog).authTableQuery(readIdentifier, null);
        verify(catalog).rollbackTo(table, instant);
        verify(catalog, never()).rollbackTo(readIdentifier, instant);
    }

    @Test
    public void testDirectReadUsesTableAsRoot() {
        Identifier table = Identifier.create("db", "my_table");
        CatalogEnvironment environment =
                new CatalogEnvironment(table, null, null, null, null, null, false, false);

        assertThat(environment.readVia()).isEmpty();
        assertThat(environment.readIdentifier()).isEqualTo(table);
        assertThat(environment.readRoot()).isEqualTo(table);
    }

    @Test
    public void testCopyAndSerializationPreserveReadVia() throws Exception {
        Identifier table = Identifier.create("db", "my_table");
        Identifier view = Identifier.create("db", "my_view");
        Identifier branchTable = new Identifier("db", "my_table", "branch1");
        CatalogEnvironment environment =
                new CatalogEnvironment(table, view, null, null, null, null, null, false, false);

        CatalogEnvironment copied = environment.copy(branchTable);
        CatalogEnvironment restored = InstantiationUtil.clone(copied);

        assertThat(restored.identifier()).isEqualTo(branchTable);
        assertThat(restored.readVia()).contains(view);
        assertThat(restored.readIdentifier()).isEqualTo(RESTReadVia.withReadVia(branchTable, view));
    }

    @Test
    public void testSnapshotLoaderBranchCopyPreservesReadVia() throws Exception {
        Identifier table = Identifier.create("db", "my_table");
        Identifier view = Identifier.create("db", "my_view");
        Identifier branchTable = new Identifier("db", "my_table", "branch1");
        Identifier branchReadIdentifier = RESTReadVia.withReadVia(branchTable, view);
        Catalog catalog = mock(Catalog.class);
        when(catalog.loadSnapshot(branchReadIdentifier)).thenReturn(Optional.empty());
        CatalogEnvironment environment =
                new CatalogEnvironment(
                        table, view, null, () -> catalog, null, null, null, true, false);

        SnapshotLoader branchLoader = environment.snapshotLoader().copyWithBranch("branch1");
        branchLoader.load();
        Instant instant = Instant.snapshot(1L);
        branchLoader.rollback(instant);

        verify(catalog).loadSnapshot(branchReadIdentifier);
        verify(catalog).rollbackTo(branchTable, instant);
    }

    @Test
    public void testReferenceReadRootOnlyWhenEnabled() {
        Identifier table = Identifier.create("db", "my_table");
        Identifier view = Identifier.create("db", "my_view");
        Options enabled = new Options();
        enabled.set(RESTCatalogOptions.READ_VIA_ENABLED, true);
        CatalogEnvironment direct =
                new CatalogEnvironment(
                        table,
                        null,
                        null,
                        null,
                        null,
                        null,
                        CatalogContext.create(enabled),
                        false,
                        false);
        CatalogEnvironment throughView =
                new CatalogEnvironment(
                        table,
                        view,
                        null,
                        null,
                        null,
                        null,
                        CatalogContext.create(enabled),
                        false,
                        false);
        CatalogEnvironment disabled =
                new CatalogEnvironment(
                        table,
                        view,
                        null,
                        null,
                        null,
                        null,
                        CatalogContext.create(new Options()),
                        false,
                        false);

        assertThat(direct.readRootForReferences()).isEqualTo(table);
        assertThat(throughView.readRootForReferences()).isEqualTo(view);
        assertThat(disabled.readRootForReferences()).isNull();
    }
}
