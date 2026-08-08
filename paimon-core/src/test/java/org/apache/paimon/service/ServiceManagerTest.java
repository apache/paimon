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

package org.apache.paimon.service;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.service.ServiceManager.SERVICE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

class ServiceManagerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void test() throws IOException {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toUri());
        ServiceManager manager = new ServiceManager(fileIO, path);
        InetSocketAddress[] addresses =
                new InetSocketAddress[] {
                    new InetSocketAddress("127.0.0.1", 7000),
                    new InetSocketAddress("127.0.0.1", 7500)
                };
        manager.resetService(PRIMARY_KEY_LOOKUP, addresses);

        String fileString =
                fileIO.readFileUtf8(
                        new Path(new Path(path, "service"), SERVICE_PREFIX + PRIMARY_KEY_LOOKUP));
        assertThat(fileString).isEqualTo("[ \"127.0.0.1:7000\", \"127.0.0.1:7500\" ]");

        Optional<InetSocketAddress[]> result = manager.service(PRIMARY_KEY_LOOKUP);
        assertThat(result).hasValue(addresses);
    }

    @Test
    public void testOwnerScopedPublicationAndCloseFencing() {
        ServiceManager manager = manager();
        String serviceId = "global-index-test";
        String oldOwner = "0000000000000000001-0000000000-old";
        String newOwner = "0000000000000000001-0000000001-new";
        GlobalIndexQueryServiceDescriptor oldAttempt = descriptor(oldOwner, true, "old");
        GlobalIndexQueryServiceDescriptor newAttempt = descriptor(newOwner, false, "new attempt");

        manager.resetGlobalIndexService(serviceId, oldAttempt);
        manager.resetGlobalIndexService(serviceId, newAttempt);
        // A delayed write and close from the old attempt cannot replace or delete the newer owner.
        manager.resetGlobalIndexService(serviceId, oldAttempt);
        assertThat(manager.globalIndexService(serviceId)).contains(newAttempt);
        manager.deleteGlobalIndexServiceIfOwned(serviceId, oldOwner);
        assertThat(manager.globalIndexService(serviceId)).contains(newAttempt);

        manager.deleteGlobalIndexServiceIfOwned(serviceId, newOwner);
        assertThat(manager.globalIndexService(serviceId))
                .hasValueSatisfying(
                        tombstone -> {
                            assertThat(tombstone.ownerToken()).isEqualTo(newOwner);
                            assertThat(tombstone.ready()).isFalse();
                            assertThat(tombstone.reason()).contains("publisher is closed");
                        });
        // The highest owner token remains as a tombstone, so an even later old READY write cannot
        // revive discovery.
        manager.resetGlobalIndexService(serviceId, oldAttempt);
        assertThat(manager.globalIndexService(serviceId))
                .hasValueSatisfying(
                        tombstone -> {
                            assertThat(tombstone.ownerToken()).isEqualTo(newOwner);
                            assertThat(tombstone.ready()).isFalse();
                        });
        assertThat(manager.nextGlobalIndexOwnerSequence(serviceId)).isEqualTo(2L);
    }

    @Test
    public void testSameSequenceHasDeterministicTieBreak() {
        ServiceManager manager = manager();
        String serviceId = "global-index-same-sequence";
        GlobalIndexQueryServiceDescriptor lower =
                descriptor("0000000000000000005-0000000000-aaa", true, "lower");
        GlobalIndexQueryServiceDescriptor higher =
                descriptor("0000000000000000005-0000000000-bbb", false, "higher");

        manager.resetGlobalIndexService(serviceId, lower);
        manager.resetGlobalIndexService(serviceId, higher);
        manager.resetGlobalIndexService(serviceId, lower);

        assertThat(manager.globalIndexService(serviceId)).contains(higher);
        assertThat(manager.nextGlobalIndexOwnerSequence(serviceId)).isEqualTo(6L);
    }

    @Test
    public void testOwnerClaimRemovesCanonicalCompatibilityDescriptor() throws IOException {
        LocalFileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.resolve("legacy").toUri());
        ServiceManager manager = new ServiceManager(fileIO, tablePath);
        String serviceId = "global-index-legacy";
        GlobalIndexQueryServiceDescriptor legacy = descriptor("legacy-owner", true, "legacy");
        Path canonicalPath =
                new Path(new Path(tablePath, "service"), ServiceManager.SERVICE_PREFIX + serviceId);
        fileIO.overwriteFileUtf8(canonicalPath, JsonSerdeUtil.toJson(legacy));
        assertThat(manager.globalIndexService(serviceId)).contains(legacy);

        GlobalIndexQueryServiceDescriptor replacement =
                descriptor("0000000000000000001-0000000000-replacement", false, "replacement");
        manager.resetGlobalIndexService(serviceId, replacement);
        assertThat(fileIO.exists(canonicalPath)).isFalse();
        manager.deleteGlobalIndexServiceIfOwned(serviceId, replacement.ownerToken());

        assertThat(manager.globalIndexService(serviceId))
                .hasValueSatisfying(
                        tombstone -> {
                            assertThat(tombstone.ownerToken()).isEqualTo(replacement.ownerToken());
                            assertThat(tombstone.ready()).isFalse();
                        });
    }

    private ServiceManager manager() {
        return new ServiceManager(LocalFileIO.create(), new Path(tempDir.toUri()));
    }

    private GlobalIndexQueryServiceDescriptor descriptor(
            String ownerToken, boolean ready, String reason) {
        return new GlobalIndexQueryServiceDescriptor(
                GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION,
                "table-uuid",
                "main",
                1L,
                "schema-fingerprint",
                1,
                new int[] {2},
                3L,
                3L,
                "snapshot-uuid",
                GlobalIndexQueryServiceDescriptor.KEY_HASH_VERSION,
                GlobalIndexQueryServiceDescriptor.LAYOUT,
                ownerToken,
                ready,
                reason,
                new GlobalIndexQueryServiceDescriptor.Endpoint[0]);
    }
}
