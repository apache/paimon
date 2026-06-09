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

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableWriteCoordinatorTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLatestIdentifierAndScan(boolean initSnapshot) throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // initial with snapshot 1
        if (initSnapshot) {
            write(table, GenericRow.of(1));
        }
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);

        // latest snapshot get snapshot 2
        write(table, GenericRow.of(1));
        Snapshot latest = table.latestSnapshot().get();
        String commitUser = latest.commitUser();
        coordinator.latestCommittedIdentifier(commitUser);

        // scan should scan snapshot 2
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(latest.id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(initSnapshot ? 2 : 1);
    }

    @Test
    public void testPrefetchManifestsWarmsCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PREFETCH_MANIFESTS
                                        .key(),
                                "true")
                        .build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        write(table, GenericRow.of(1));
        write(table, GenericRow.of(2));

        // reset the manifest cache to a fresh, cold instance (the writes above may have populated
        // it) so we can assert that constructing the coordinator is what warms it
        // the existing cache on the table comes from CachingCatalog, which is distinct from
        // TableWriteCoordinator
        SegmentsCache<Path> cache = table.getManifestCache();
        table.setManifestCache(
                SegmentsCache.create(
                        cache.pageSize(),
                        cache.maxMemorySize(),
                        cache.maxElementSize(),
                        cache.ttl(),
                        cache.softValues()));
        assertThat(table.getManifestCache().totalCacheBytes()).isZero();

        // constructing the coordinator runs refresh() which warms the manifest cache when the
        // prefetch option is enabled
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        assertThat(table.getManifestCache().totalCacheBytes()).isGreaterThan(0);

        // scan results remain correct after warming
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(table.latestSnapshot().get().id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(2);
    }

    @Test
    public void testBuildManifestCacheOptions() {
        // by default soft values are on and there is no idle TTL; the cache is bounded by memory
        Options defaults = new Options();
        SegmentsCache<Path> cache = WriteOperatorCoordinator.buildManifestCache(defaults);
        assertThat(cache.softValues()).isTrue();
        assertThat(cache.ttl()).isNull();
        assertThat(cache.maxMemorySize())
                .isEqualTo(SINK_WRITER_COORDINATOR_CACHE_MEMORY.defaultValue());

        // an explicit expire-after-access TTL is honored
        Options withTtl = new Options();
        withTtl.set(SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS, Duration.ofMinutes(5));
        cache = WriteOperatorCoordinator.buildManifestCache(withTtl);
        assertThat(cache.ttl()).isEqualTo(Duration.ofMinutes(5));
        assertThat(cache.softValues()).isTrue();

        // disabling soft values switches to strong references; still no TTL by default
        Options strongRefs = new Options();
        strongRefs.set(SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES, false);
        cache = WriteOperatorCoordinator.buildManifestCache(strongRefs);
        assertThat(cache.softValues()).isFalse();
        assertThat(cache.ttl()).isNull();

        // a zero cache memory disables the cache entirely
        Options noCache = new Options();
        noCache.set(SINK_WRITER_COORDINATOR_CACHE_MEMORY, MemorySize.ofBytes(0));
        assertThat(WriteOperatorCoordinator.buildManifestCache(noCache)).isNull();
    }

    @Test
    public void testNoManifestCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        catalog.createDatabase("db", false);
        createTable(identifier);
        FileStoreTable table = getTable(identifier);
        table.setManifestCache(null);
        assertThatThrownBy(() -> new TableWriteCoordinator(table))
                .isInstanceOf(NullPointerException.class);
    }
}
