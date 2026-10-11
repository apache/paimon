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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TestCatalogLockFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests Hive policy and factory context without connecting to a metastore. */
class HiveCatalogLockConfigurationTest {
    @ParameterizedTest
    @CsvSource({"false,,false", "true,,true", "false,true,true", "true,false,false"})
    void hivePreservesStorageDefaultAndExplicitOverride(
            boolean objectStore, Boolean enabled, boolean expected) throws Exception {
        Options options = new Options();
        if (enabled != null) {
            options.set(HiveCatalogOptions.LOCK_ENABLED, enabled);
        }
        try (HiveCatalog catalog = catalog(options, objectStore);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"),
                                "table-id",
                                "writer",
                                new Options())) {
            assertThat(lock.runWithLock(() -> options.getString("test.lock.held", "false")))
                    .isEqualTo(Boolean.toString(expected));
        }
        assertThat(options.getString("test.lock.held", "false")).isEqualTo("false");
        assertThat(options.getString("test.lock.closed", "false"))
                .isEqualTo(Boolean.toString(expected));
    }

    @Test
    void configuredFactoryOverridesHiveDefault() throws Exception {
        Options options = new Options();
        options.set(HiveCatalogOptions.LOCK_ENABLED, true);
        options.set(HiveCatalogOptions.LOCK_TYPE, TestCatalogLockFactory.IDENTIFIER);
        try (HiveCatalog catalog = catalog(options, false);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"), null, "writer", new Options())) {
            assertThat(lock.runWithLock(() -> options.get("test.lock.held"))).isEqualTo("true");
        }
        assertThat(options.get("test.lock.held")).isEqualTo("false");
    }

    @Test
    void disabledHiveLockDoesNotResolveFactory() throws Exception {
        Options options = new Options();
        options.set(HiveCatalogOptions.LOCK_ENABLED, false);
        options.set(HiveCatalogOptions.LOCK_TYPE, "not-installed");
        try (HiveCatalog catalog = catalog(options, true);
                Lock lock =
                        catalog.createLock(
                                Identifier.create("db", "table"), null, "writer", new Options())) {
            assertThat(lock.runWithLock(() -> "published")).isEqualTo("published");
        }
    }

    @Test
    void durationOptionsConfigureHiveLocks() {
        HiveConf conf = new HiveConf();
        conf.set(HiveCatalogOptions.LOCK_CHECK_MAX_SLEEP.key(), "17 ms");
        conf.set(HiveCatalogOptions.LOCK_ACQUIRE_TIMEOUT.key(), "3 s");
        assertThat(HiveCatalogLock.checkMaxSleep(conf)).isEqualTo(17);
        assertThat(HiveCatalogLock.acquireTimeout(conf)).isEqualTo(3000);
        HiveConf defaults = new HiveConf();
        assertThat(HiveCatalogLock.checkMaxSleep(defaults)).isEqualTo(8000);
        assertThat(HiveCatalogLock.acquireTimeout(defaults)).isEqualTo(480000);
    }

    private HiveCatalog catalog(Options options, boolean objectStore) {
        FileIO fileIO = mock(FileIO.class);
        when(fileIO.isObjectStore()).thenReturn(objectStore);
        return new HiveCatalog(
                fileIO,
                new HiveConf(),
                "unused",
                CatalogContext.create(options),
                "file:/warehouse") {
            @Override
            public Optional<CatalogLockFactory> defaultLockFactory() {
                if (options.contains(HiveCatalogOptions.LOCK_TYPE)) {
                    throw new AssertionError("Explicit factory must override the default.");
                }
                return Optional.of(new TestCatalogLockFactory());
            }
        };
    }
}
