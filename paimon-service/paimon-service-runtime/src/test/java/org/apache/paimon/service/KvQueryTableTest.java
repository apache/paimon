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

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.options.Options;
import org.apache.paimon.query.QueryLocationImpl;
import org.apache.paimon.service.client.KvQueryClient;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.query.TableQuery;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.table.sink.ChannelComputer.select;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for remote lookup. */
public class KvQueryTableTest extends PrimaryKeyTableTestBase {

    private LocalTableQuery query0;
    private LocalTableQuery query1;

    private KvQueryServer server0;
    private KvQueryServer server1;

    private KvQueryClient client;

    @Override
    protected Options tableOptions() {
        Options options = new Options();
        options.set("bucket", "1");
        return options;
    }

    @BeforeEach
    public void beforeEach() {
        IOManager ioManager = IOManager.create(tempPath.toString());
        this.query0 = table.newLocalTableQuery().withIOManager(ioManager);
        this.query1 = table.newLocalTableQuery().withIOManager(ioManager);

        this.server0 = createServer(0, query0, 7700, 7799);
        this.server1 = createServer(1, query1, 7900, 7999);
        registryServers();

        this.client =
                new KvQueryClient(new QueryLocationImpl(table.store().newServiceManager()), 1);
    }

    private void registryServers() {
        InetSocketAddress[] addresses =
                new InetSocketAddress[] {server0.getServerAddress(), server1.getServerAddress()};
        ServiceManager serviceManager = table.store().newServiceManager();
        serviceManager.resetService(PRIMARY_KEY_LOOKUP, addresses);
    }

    private KvQueryServer createServer(int serverId, TableQuery query, int minPort, int maxPort) {
        try {
            List<Integer> portList = new ArrayList<>();
            for (int p = minPort; p <= maxPort; p++) {
                portList.add(p);
            }
            KvQueryServer server =
                    new KvQueryServer(
                            serverId,
                            2,
                            InetAddress.getLocalHost().getHostName(),
                            portList.iterator(),
                            1,
                            1,
                            query,
                            new DisabledServiceRequestStats());
            server.start();
            return server;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void afterEach() throws Exception {
        shutdownServers();
        if (client != null) {
            client.shutdownFuture().get();
        }
    }

    private void shutdownServers() {
        if (server0 != null) {
            try {
                server0.shutdownServer().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (server1 != null) {
            try {
                server1.shutdownServer().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testRemoteGet() throws Exception {
        // test not exists
        BinaryRow[] result = client.getValues(row(1), 0, new BinaryRow[] {row(1)}).get();
        assertThat(result).containsOnly((BinaryRow) null);

        // test 1 row
        write(1, 1, 1);
        result = client.getValues(row(1), 0, new BinaryRow[] {row(1)}).get();
        assertThat(result).containsOnly(row(1, 1, 1));

        // test many partitions
        for (int i = 2; i < 10; i++) {
            write(i, 1, 2);
            result = client.getValues(row(i), 0, new BinaryRow[] {row(1)}).get();
            assertThat(result).containsOnly(row(i, 1, 2));
        }

        // test 2 rows
        write(1, 2, 1);
        result = client.getValues(row(1), 0, new BinaryRow[] {row(1), row(2)}).get();
        assertThat(result).containsOnly(row(1, 1, 1), row(1, 2, 1));
    }

    @Test
    public void testServerRestartSamePorts() throws Throwable {
        innerTestServerRestart(
                () -> {
                    shutdownServers();
                    KvQueryTableTest.this.server0 = createServer(0, query0, 7700, 7799);
                    KvQueryTableTest.this.server1 = createServer(1, query1, 7900, 7999);
                    registryServers();
                });
    }

    @Test
    public void testServerRestartSwitchPorts() throws Throwable {
        innerTestServerRestart(
                () -> {
                    shutdownServers();
                    KvQueryTableTest.this.server0 = createServer(0, query0, 7900, 7999);
                    KvQueryTableTest.this.server1 = createServer(1, query1, 7700, 7799);
                    registryServers();
                });
    }

    @Test
    public void testServerRestartChangePorts() throws Throwable {
        innerTestServerRestart(
                () -> {
                    shutdownServers();
                    KvQueryTableTest.this.server0 = createServer(0, query0, 7700, 7799);
                    KvQueryTableTest.this.server1 = createServer(1, query1, 7900, 7999);
                    registryServers();
                });
    }

    private void innerTestServerRestart(Runnable restart) throws Throwable {
        // insert many records
        BinaryRow[] result;
        for (int i = 1; i < 10; i++) {
            write(i, 1, 2);
            result = client.getValues(row(i), 0, new BinaryRow[] {row(1)}).get();
            assertThat(result).containsOnly(row(i, 1, 2));
        }

        restart.run();

        // query again
        for (int i = 1; i < 10; i++) {
            result = client.getValues(row(i), 0, new BinaryRow[] {row(1)}).get();
            assertThat(result).containsOnly(row(i, 1, 2));
        }
    }

    private void write(int partition, int key, int value) throws Exception {
        int bucket = computeBucket(partition, key, value);
        LocalTableQuery query = select(row(partition), bucket, 2) == 0 ? query0 : query1;
        BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
        write.write(row(partition, key, value), bucket);
        CommitMessageImpl message = (CommitMessageImpl) write.prepareCommit().get(0);
        query.refreshFiles(
                message.partition(),
                message.bucket(),
                Collections.emptyList(),
                message.newFilesIncrement().newFiles());
    }

    private int computeBucket(int partition, int key, int value) throws Exception {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite()) {
            write.write(GenericRow.of(partition, key, value));
            List<CommitMessage> commitMessages = write.prepareCommit();
            return commitMessages.get(0).bucket();
        }
    }
}
