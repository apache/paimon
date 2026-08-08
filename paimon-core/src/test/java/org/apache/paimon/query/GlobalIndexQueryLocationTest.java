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

package org.apache.paimon.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor;
import org.apache.paimon.service.GlobalIndexQueryServiceDescriptor.Endpoint;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.query.QueryServiceNotReadyException;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.stream.IntStream;

import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.route;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests versioned, snapshot-fenced global-index service discovery. */
public class GlobalIndexQueryLocationTest extends TableTestBase {

    @Test
    public void testWideProjectionUsesBoundedStableServiceId() {
        int[] wideProjection = IntStream.range(0, 200).toArray();
        String serviceId = ServiceManager.globalIndexLookupService(7L, 3, wideProjection);

        assertThat(serviceId).hasSizeLessThan(100);
        assertThat(ServiceManager.globalIndexLookupService(7L, 3, wideProjection))
                .isEqualTo(serviceId);
        int[] reordered = wideProjection.clone();
        int value = reordered[0];
        reordered[0] = reordered[1];
        reordered[1] = value;
        assertThat(ServiceManager.globalIndexLookupService(7L, 3, reordered))
                .isNotEqualTo(serviceId);
    }

    @Test
    public void testDiscoveryValidationAndRouting() throws Exception {
        FileStoreTable table = createTable();
        QuerySpec spec =
                GlobalIndexQueryServiceUtils.querySpec(
                        table, "url", Collections.singletonList("descriptor"));
        ServiceManager manager = table.store().newServiceManager();
        InetSocketAddress[] addresses =
                new InetSocketAddress[] {
                    new InetSocketAddress("127.0.0.1", 7000),
                    new InetSocketAddress("127.0.0.1", 7001)
                };
        String[] epochs = new String[] {"epoch-0", "epoch-1"};
        manager.resetGlobalIndexService(
                spec.serviceId(),
                descriptor(table, spec, true, "", spec.schemaFingerprint(), addresses, epochs));

        GlobalIndexQueryLocationImpl location =
                new GlobalIndexQueryLocationImpl(
                        manager,
                        table.uuid(),
                        table.coreOptions().branch(),
                        table.schema().id(),
                        spec);
        BinaryRow key = key("O1CN.jpg");
        int shard = route(key, addresses.length);
        GlobalIndexQueryEndpoint endpoint = location.getLocation(key, false);
        assertThat(endpoint.shardId()).isEqualTo(shard);
        assertThat(endpoint.address()).isEqualTo(addresses[shard]);
        assertThat(endpoint.serverEpoch()).isEqualTo(epochs[shard]);
        assertThat(endpoint.servedGeneration()).isEqualTo(12L);
        assertThat(endpoint.servedSnapshotId()).isEqualTo(12L);
        assertThat(endpoint.snapshotUuid()).isEqualTo("snapshot-uuid");
        assertThat(location.isServiceReady()).isTrue();

        manager.deleteGlobalIndexServiceIfOwned(spec.serviceId(), "owner");
        assertThat(location.isServiceReady()).isFalse();
        manager.resetGlobalIndexService(
                spec.serviceId(),
                descriptor(table, spec, true, "", spec.schemaFingerprint(), addresses, epochs));

        // Snapshot UUID was added after snapshot IDs. Legacy snapshots remain safely fenced by
        // generation, server epoch and schema even when no UUID is available.
        manager.resetGlobalIndexService(
                spec.serviceId(),
                new GlobalIndexQueryServiceDescriptor(
                        GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                        table.uuid(),
                        table.coreOptions().branch(),
                        table.schema().id(),
                        spec.schemaFingerprint(),
                        spec.lookupFieldId(),
                        spec.valueFieldIds(),
                        13L,
                        12L,
                        null,
                        GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                        GlobalIndexQueryServiceDescriptor.LAYOUT,
                        "owner",
                        true,
                        "",
                        endpoints(addresses, epochs)));
        GlobalIndexQueryEndpoint legacyEndpoint = location.getLocation(key, true);
        assertThat(legacyEndpoint.servedGeneration()).isEqualTo(13L);
        assertThat(legacyEndpoint.servedSnapshotId()).isEqualTo(12L);
        assertThat(legacyEndpoint.snapshotUuid()).isNull();

        manager.resetGlobalIndexService(
                spec.serviceId(),
                descriptor(
                        table,
                        spec,
                        false,
                        "bootstrap incomplete",
                        spec.schemaFingerprint(),
                        new InetSocketAddress[0],
                        new String[0]));
        assertThatThrownBy(() -> location.getLocation(key, true))
                .isInstanceOf(QueryServiceNotReadyException.class)
                .hasMessageContaining("bootstrap incomplete");
        assertThat(location.isServiceReady()).isFalse();

        manager.resetGlobalIndexService(
                spec.serviceId(), descriptor(table, spec, true, "", "wrong", addresses, epochs));
        assertThatThrownBy(() -> location.getLocation(key, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("schema does not match");

        manager.resetGlobalIndexService(
                spec.serviceId(),
                new GlobalIndexQueryServiceDescriptor(
                        GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                        "recreated-table-uuid",
                        table.coreOptions().branch(),
                        table.schema().id(),
                        spec.schemaFingerprint(),
                        spec.lookupFieldId(),
                        spec.valueFieldIds(),
                        13L,
                        12L,
                        "snapshot-uuid",
                        GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                        GlobalIndexQueryServiceDescriptor.LAYOUT,
                        "owner",
                        true,
                        "",
                        endpoints(addresses, epochs)));
        assertThatThrownBy(() -> location.getLocation(key, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("table identity does not match");

        Endpoint[] reversed =
                new Endpoint[] {
                    new Endpoint(1, addresses[0], epochs[0]),
                    new Endpoint(0, addresses[1], epochs[1])
                };
        manager.resetGlobalIndexService(
                spec.serviceId(), descriptor(table, spec, true, "", reversed));
        assertThatThrownBy(() -> location.getLocation(key, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("invalid endpoint");
        assertThat(location.isServiceReady()).isFalse();
    }

    private FileStoreTable createTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("url", DataTypes.STRING())
                        .column("descriptor", DataTypes.BYTES())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .build();
        catalog.createTable(identifier(), schema, false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            FileStoreTable table,
            QuerySpec spec,
            boolean ready,
            String reason,
            String fingerprint,
            InetSocketAddress[] addresses,
            String[] epochs) {
        return new GlobalIndexQueryServiceDescriptor(
                GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                table.uuid(),
                table.coreOptions().branch(),
                table.schema().id(),
                fingerprint,
                spec.lookupFieldId(),
                spec.valueFieldIds(),
                12L,
                12L,
                "snapshot-uuid",
                GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                GlobalIndexQueryServiceDescriptor.LAYOUT,
                "owner",
                ready,
                reason,
                endpoints(addresses, epochs));
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            FileStoreTable table,
            QuerySpec spec,
            boolean ready,
            String reason,
            Endpoint[] endpoints) {
        return descriptor(table, spec, ready, reason, endpoints, "owner");
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            FileStoreTable table,
            QuerySpec spec,
            boolean ready,
            String reason,
            Endpoint[] endpoints,
            String ownerToken) {
        return new GlobalIndexQueryServiceDescriptor(
                GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                table.uuid(),
                table.coreOptions().branch(),
                table.schema().id(),
                spec.schemaFingerprint(),
                spec.lookupFieldId(),
                spec.valueFieldIds(),
                12L,
                12L,
                "snapshot-uuid",
                GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                GlobalIndexQueryServiceDescriptor.LAYOUT,
                ownerToken,
                ready,
                reason,
                endpoints);
    }

    private Endpoint[] endpoints(InetSocketAddress[] addresses, String[] epochs) {
        Endpoint[] endpoints = new Endpoint[addresses.length];
        for (int i = 0; i < endpoints.length; i++) {
            endpoints[i] = new Endpoint(i, addresses[i], epochs[i]);
        }
        return endpoints;
    }

    private BinaryRow key(String value) {
        InternalRowSerializer serializer =
                InternalSerializers.create(RowType.of(DataTypes.STRING()));
        return serializer.toBinaryRow(GenericRow.of(BinaryString.fromString(value))).copy();
    }
}
