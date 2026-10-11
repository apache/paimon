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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.jdbc.JdbcCatalogOptions;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/** Tests that catalogs without a locking policy ignore backend-specific lock options. */
class CatalogLockConfigurationTest {
    @Test
    void abstractCatalogDoesNotInterpretBackendLockOptions() throws Exception {
        Options options = new Options();
        options.set(JdbcCatalogOptions.LOCK_ENABLED, true);
        options.set(JdbcCatalogOptions.LOCK_TYPE, "not-installed");
        AbstractCatalog catalog =
                mock(
                        AbstractCatalog.class,
                        withSettings()
                                .useConstructor(mock(FileIO.class), CatalogContext.create(options))
                                .defaultAnswer(CALLS_REAL_METHODS));
        try (Lock lock =
                catalog.createLock(
                        Identifier.create("db", "table"), null, "writer", new Options())) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void filesystemDoesNotInterpretBackendLockOptions(boolean objectStore) throws Exception {
        Options options = new Options();
        options.set(JdbcCatalogOptions.LOCK_ENABLED, true);
        options.set(JdbcCatalogOptions.LOCK_TYPE, "not-installed");
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.isObjectStore()).thenReturn(objectStore);
        FileSystemCatalog catalog =
                new FileSystemCatalog(
                        fileIO, new Path("file:/warehouse"), CatalogContext.create(options));
        Identifier identifier = Identifier.create("db", "table");
        try (Lock lock = catalog.createLock(identifier, "table-id", "writer", new Options())) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
        }
        assertThat(catalog.runWithLock(identifier, () -> "created")).isEqualTo("created");
    }

    @Test
    void factoryBindsRuntimeIdentityWhenCatalogCreatesLock() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set("catalog-option", "value");
        Options tableOptions = new Options();
        tableOptions.set("runtime-option", "dynamic-value");
        Identifier identifier = new Identifier("db", "table", "dev");
        CatalogLockFactory factory = mock(CatalogLockFactory.class);
        Lock backend = mock(Lock.class);
        when(backend.runWithLock(any()))
                .thenAnswer(invocation -> ((Callable<?>) invocation.getArgument(0)).call());
        when(factory.createLock(
                        any(), eq(identifier), eq("table-id"), eq("writer"), eq(tableOptions)))
                .thenAnswer(
                        invocation -> {
                            CatalogLockContext context = invocation.getArgument(0);
                            assertThat(context.options()).isEqualTo(catalogOptions);
                            return backend;
                        });
        AbstractCatalog catalog =
                mock(
                        AbstractCatalog.class,
                        withSettings()
                                .useConstructor(
                                        mock(FileIO.class), CatalogContext.create(catalogOptions))
                                .defaultAnswer(CALLS_REAL_METHODS));
        when(catalog.lockFactory()).thenReturn(Optional.of(factory));
        try (Lock lock = catalog.createLock(identifier, "table-id", "writer", tableOptions)) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
        }
        verify(factory)
                .createLock(any(), eq(identifier), eq("table-id"), eq("writer"), eq(tableOptions));
        verify(backend).ensureValid();
        verify(backend).close();
    }
}
