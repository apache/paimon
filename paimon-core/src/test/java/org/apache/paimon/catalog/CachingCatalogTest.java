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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.FakeTicker;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_MAX_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY;
import static org.apache.paimon.options.CatalogOptions.CACHE_MANIFEST_SMALL_FILE_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CachingCatalogTest extends CatalogTestBase {

    private static final Duration EXPIRATION_TTL = Duration.ofMinutes(5);
    private static final Duration HALF_OF_EXPIRATION = EXPIRATION_TTL.dividedBy(2);

    private FakeTicker ticker;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog = new FileSystemCatalog(fileIO, new Path(warehouse));
        ticker = new FakeTicker();
        catalog.createDatabase("db", false);
    }

    @Override
    @Test
    public void testListDatabasesWhenNoDatabases() {
        List<String> databases = catalog.listDatabases();
        assertThat(databases).contains("db");
    }

    @Test
    public void testInvalidateSystemTablesIfBaseTableIsModified() throws Exception {
        Catalog catalog = new CachingCatalog(this.catalog);
        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(new Identifier("db", "tbl"), DEFAULT_TABLE_SCHEMA, false);
        Identifier sysIdent = new Identifier("db", "tbl$files");
        Table sysTable = catalog.getTable(sysIdent);
        catalog.alterTable(tableIdent, SchemaChange.addColumn("col3", DataTypes.INT()), false);
        assertThat(catalog.getTable(sysIdent)).isNotSameAs(sysTable);
    }

    @Test
    public void testInvalidateSysTablesIfBaseTableIsDropped() throws Exception {
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);
        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(new Identifier("db", "tbl"), DEFAULT_TABLE_SCHEMA, false);
        Identifier sysIdent = new Identifier("db", "tbl$files");
        // get system table will only cache the origin table
        catalog.getTable(sysIdent);
        assertThat(catalog.tableCache.asMap()).containsKey(tableIdent);
        assertThat(catalog.tableCache.asMap()).doesNotContainKey(sysIdent);
        // test case sensitivity
        Identifier sysIdent1 = new Identifier("db", "tbl$SNAPSHOTS");
        catalog.getTable(sysIdent1);
        assertThat(catalog.tableCache.asMap()).doesNotContainKey(sysIdent1);

        catalog.dropTable(tableIdent, false);
        assertThat(catalog.tableCache.asMap()).doesNotContainKey(tableIdent);
        assertThatThrownBy(() -> catalog.getTable(sysIdent))
                .hasMessage("Table db.tbl does not exist.");
        assertThatThrownBy(() -> catalog.getTable(sysIdent1))
                .hasMessage("Table db.tbl does not exist.");
    }

    @Test
    public void testInvalidateBranchIfBaseTableIsDropped() throws Exception {
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);
        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(new Identifier("db", "tbl"), DEFAULT_TABLE_SCHEMA, false);
        catalog.getTable(tableIdent).createBranch("b1");

        Identifier branchIdent = new Identifier("db", "tbl$branch_b1");
        Identifier branchSysIdent = new Identifier("db", "tbl$branch_b1$FILES");
        // get system table will only cache the origin table
        catalog.getTable(branchSysIdent);
        assertThat(catalog.tableCache.asMap()).containsKey(branchIdent);
        assertThat(catalog.tableCache.asMap()).doesNotContainKey(branchSysIdent);

        catalog.dropTable(tableIdent, false);
        assertThat(catalog.tableCache.asMap()).doesNotContainKey(branchIdent);
        assertThatThrownBy(() -> catalog.getTable(branchIdent))
                .hasMessage("Table db.tbl$branch_b1 does not exist.");
        assertThatThrownBy(() -> catalog.getTable(branchSysIdent))
                .hasMessage("Table db.tbl$branch_b1 does not exist.");
    }

    @Test
    public void testTableExpiresAfterInterval() throws Exception {
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);

        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);
        Table table = catalog.getTable(tableIdent);

        // Ensure table is cached with full ttl remaining upon creation
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        assertThat(catalog.remainingAgeFor(tableIdent)).isPresent().get().isEqualTo(EXPIRATION_TTL);

        ticker.advance(HALF_OF_EXPIRATION);
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(HALF_OF_EXPIRATION);

        ticker.advance(HALF_OF_EXPIRATION.plus(Duration.ofSeconds(10)));
        assertThat(catalog.tableCache().asMap()).doesNotContainKey(tableIdent);
        assertThat(catalog.getTable(tableIdent))
                .as("CachingCatalog should return a new instance after expiration")
                .isNotSameAs(table);
    }

    @Test
    public void testCatalogExpirationTtlRefreshesAfterAccessViaCatalog() throws Exception {
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);

        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);
        catalog.getTable(tableIdent);
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(Duration.ZERO);

        ticker.advance(HALF_OF_EXPIRATION);
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(HALF_OF_EXPIRATION);
        assertThat(catalog.remainingAgeFor(tableIdent))
                .isPresent()
                .get()
                .isEqualTo(HALF_OF_EXPIRATION);

        Duration oneMinute = Duration.ofMinutes(1L);
        ticker.advance(oneMinute);
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        assertThat(catalog.ageOf(tableIdent))
                .isPresent()
                .get()
                .isEqualTo(HALF_OF_EXPIRATION.plus(oneMinute));
        assertThat(catalog.remainingAgeFor(tableIdent))
                .get()
                .isEqualTo(HALF_OF_EXPIRATION.minus(oneMinute));

        // Access the table via the catalog, which should refresh the TTL
        Table table = catalog.getTable(tableIdent);
        assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(Duration.ZERO);
        assertThat(catalog.remainingAgeFor(tableIdent)).get().isEqualTo(EXPIRATION_TTL);

        ticker.advance(HALF_OF_EXPIRATION);
        assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);
        assertThat(catalog.remainingAgeFor(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);
    }

    @Test
    public void testPartitionCache() throws Exception {
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);

        Identifier tableIdent = new Identifier("db", "tbl");
        Schema schema =
                new Schema(
                        RowType.of(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE).getFields(),
                        singletonList("f0"),
                        emptyList(),
                        Collections.emptyMap(),
                        "");
        catalog.createTable(tableIdent, schema, false);
        List<PartitionEntry> partitionEntryList = catalog.listPartitions(tableIdent);
        assertThat(catalog.partitionCache().asMap()).containsKey(tableIdent);
        catalog.invalidateTable(tableIdent);
        catalog.refreshPartitions(tableIdent);
        assertThat(catalog.partitionCache().asMap()).containsKey(tableIdent);
        List<PartitionEntry> partitionEntryListFromCache =
                catalog.partitionCache().getIfPresent(tableIdent);
        assertThat(partitionEntryListFromCache).isNotNull();
        assertThat(partitionEntryListFromCache).containsAll(partitionEntryList);
    }

    @Test
    public void testDeadlock() throws Exception {
        Catalog underlyCatalog = this.catalog;
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(this.catalog, Duration.ofSeconds(1), ticker);
        int numThreads = 20;
        List<Identifier> createdTables = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            Identifier tableIdent = new Identifier("db", "tbl" + i);
            catalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);
            createdTables.add(tableIdent);
        }

        Cache<Identifier, Table> cache = catalog.tableCache();
        AtomicInteger cacheGetCount = new AtomicInteger(0);
        AtomicInteger cacheCleanupCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            if (i % 2 == 0) {
                String table = "tbl" + i;
                executor.submit(
                        () -> {
                            ticker.advance(Duration.ofSeconds(2));
                            cache.get(
                                    new Identifier("db", table),
                                    identifier -> {
                                        try {
                                            return underlyCatalog.getTable(identifier);
                                        } catch (Catalog.TableNotExistException e) {
                                            throw new RuntimeException(e);
                                        }
                                    });
                            cacheGetCount.incrementAndGet();
                        });
            } else {
                executor.submit(
                        () -> {
                            ticker.advance(Duration.ofSeconds(2));
                            cache.cleanUp();
                            cacheCleanupCount.incrementAndGet();
                        });
            }
        }
        executor.awaitTermination(2, TimeUnit.SECONDS);
        assertThat(cacheGetCount).hasValue(numThreads / 2);
        assertThat(cacheCleanupCount).hasValue(numThreads / 2);

        executor.shutdown();
    }

    @Test
    public void testCachingCatalogRejectsExpirationIntervalOfZero() {
        Assertions.assertThatThrownBy(
                        () -> new TestableCachingCatalog(this.catalog, Duration.ZERO, ticker))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "When cache.expiration-interval is set to negative or 0, the catalog cache should be disabled.");
    }

    @Test
    public void testInvalidateTableForChainedCachingCatalogs() throws Exception {
        TestableCachingCatalog wrappedCatalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);
        TestableCachingCatalog catalog =
                new TestableCachingCatalog(wrappedCatalog, EXPIRATION_TTL, ticker);
        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);
        catalog.getTable(tableIdent);
        assertThat(catalog.tableCache().asMap()).containsKey(tableIdent);
        catalog.dropTable(tableIdent, false);
        assertThat(catalog.tableCache().asMap()).doesNotContainKey(tableIdent);
        assertThat(wrappedCatalog.tableCache().asMap()).doesNotContainKey(tableIdent);
    }

    public static Identifier[] sysTables(Identifier tableIdent) {
        return SystemTableLoader.SYSTEM_TABLES.stream()
                .map(type -> Identifier.fromString(tableIdent.getFullName() + "$" + type))
                .toArray(Identifier[]::new);
    }

    @Test
    public void testSnapshotCache() throws Exception {
        TestableCachingCatalog wrappedCatalog =
                new TestableCachingCatalog(this.catalog, EXPIRATION_TTL, ticker);
        Identifier tableIdent = new Identifier("db", "tbl");
        wrappedCatalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);
        Table table = wrappedCatalog.getTable(tableIdent);

        // write
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, fromString("1"), fromString("1")));
            write.write(GenericRow.of(2, fromString("2"), fromString("2")));
            commit.commit(write.prepareCommit());
        }

        Snapshot snapshot = table.snapshot(1);
        assertThat(snapshot).isSameAs(table.snapshot(1));

        // copy
        Snapshot copied = table.copy(Collections.singletonMap("a", "b")).snapshot(1);
        assertThat(copied).isSameAs(snapshot);
    }

    @Test
    public void testManifestCache() throws Exception {
        innerTestManifestCache(Long.MAX_VALUE);
        assertThatThrownBy(() -> innerTestManifestCache(10))
                .hasRootCauseInstanceOf(FileNotFoundException.class);
    }

    private void innerTestManifestCache(long manifestCacheThreshold) throws Exception {
        Catalog catalog =
                new CachingCatalog(
                        this.catalog,
                        Duration.ofSeconds(10),
                        MemorySize.ofMebiBytes(1),
                        manifestCacheThreshold,
                        0L,
                        10);
        Identifier tableIdent = new Identifier("db", "tbl");
        catalog.dropTable(tableIdent, true);
        catalog.createTable(tableIdent, DEFAULT_TABLE_SCHEMA, false);

        // write
        Table table = catalog.getTable(tableIdent);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.write(GenericRow.of(1, fromString("1"), fromString("1")));
            write.write(GenericRow.of(2, fromString("2"), fromString("2")));
            commit.commit(write.prepareCommit());
        }

        // repeat read
        for (int i = 0; i < 5; i++) {
            // test copy too
            table = catalog.getTable(tableIdent).copy(Collections.singletonMap("a", "b"));
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan scan = readBuilder.newScan();
            TableRead read = readBuilder.newRead();
            read.createReader(scan.plan()).forEachRemaining(r -> {});

            // delete manifest to validate cache
            if (i == 0) {
                Path manifestPath = new Path(table.options().get("path"), "manifest");
                assertThat(fileIO.exists(manifestPath)).isTrue();
                fileIO.deleteDirectoryQuietly(manifestPath);
            }
        }
    }

    @Test
    public void testManifestCacheOptions() {
        Options options = new Options();

        CachingCatalog caching = (CachingCatalog) CachingCatalog.tryToCreate(catalog, options);
        assertThat(caching.manifestCache.maxMemorySize())
                .isEqualTo(CACHE_MANIFEST_SMALL_FILE_MEMORY.defaultValue());
        assertThat(caching.manifestCache.maxElementSize())
                .isEqualTo(CACHE_MANIFEST_SMALL_FILE_THRESHOLD.defaultValue().getBytes());

        options.set(CACHE_MANIFEST_SMALL_FILE_MEMORY, MemorySize.ofMebiBytes(100));
        options.set(CACHE_MANIFEST_SMALL_FILE_THRESHOLD, MemorySize.ofBytes(100));
        caching = (CachingCatalog) CachingCatalog.tryToCreate(catalog, options);
        assertThat(caching.manifestCache.maxMemorySize()).isEqualTo(MemorySize.ofMebiBytes(100));
        assertThat(caching.manifestCache.maxElementSize()).isEqualTo(100);

        options.set(CACHE_MANIFEST_MAX_MEMORY, MemorySize.ofMebiBytes(256));
        caching = (CachingCatalog) CachingCatalog.tryToCreate(catalog, options);
        assertThat(caching.manifestCache.maxMemorySize()).isEqualTo(MemorySize.ofMebiBytes(256));
        assertThat(caching.manifestCache.maxElementSize()).isEqualTo(Long.MAX_VALUE);
    }
}
