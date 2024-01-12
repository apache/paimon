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

package org.apache.paimon.flink;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.BlockingIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.flink.types.Row;


import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.table.sink.ChannelComputer.select;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for remote lookup join. */
public class RemoteLookupJoinITCase extends CatalogITCaseBase {

    @Override
    public List<String> ddl() {
        return Collections.singletonList("CREATE TABLE T (i INT, `proctime` AS PROCTIME())");
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testLookupRemoteTable() throws Throwable {
        sql("CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT)");
        ServiceProxy proxy = launchQueryServer("DIM");

        proxy.write(GenericRow.of(1, 11, 111, 1111));
        proxy.write(GenericRow.of(2, 22, 222, 2222));

        String query =
                "SELECT T.i, D.j, D.k1 FROM T LEFT JOIN DIM for system_time as of T.proctime AS D ON T.i = D.i";
        BlockingIterator<Row, Row> iterator = BlockingIterator.of(sEnv.executeSql(query).collect());

        sql("INSERT INTO T VALUES (1), (2), (3)");
        List<Row> result = iterator.collect(3);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111), Row.of(2, 22, 222), Row.of(3, null, null));

        proxy.write(GenericRow.of(2, 44, 444, 4444));
        proxy.write(GenericRow.of(3, 33, 333, 3333));

        Thread.sleep(2000); // wait refresh
        sql("INSERT INTO T VALUES (1), (2), (3), (4)");
        result = iterator.collect(4);
        assertThat(result)
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 44, 444),
                        Row.of(3, 33, 333),
                        Row.of(4, null, null));

        iterator.close();

        proxy.close();
    }

    private ServiceProxy launchQueryServer(String tableName) throws Throwable {
        FileStoreTable table = (FileStoreTable) getPaimonTable(tableName);
        LocalTableQuery query = table.newLocalTableQuery().withIOManager(IOManager.create(path));
        KvQueryServer server =
                new KvQueryServer(
                        0,
                        1,
                        InetAddress.getLocalHost().getHostName(),
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        query,
                        new DisabledServiceRequestStats());
        server.start();

        InetSocketAddress[] addresses =
                new InetSocketAddress[] {server.getServerAddress()};
        ServiceManager serviceManager = table.store().newServiceManager();
        serviceManager.resetService(PRIMARY_KEY_LOOKUP, addresses);

        return new ServiceProxy() {

            @Override
            public void write(InternalRow row) throws Exception {
                BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
                write.write(row);
                CommitMessageImpl message = (CommitMessageImpl) write.prepareCommit().get(0);
                query.refreshFiles(
                        message.partition(),
                        message.bucket(),
                        Collections.emptyList(),
                        message.newFilesIncrement().newFiles());
            }

            @Override
            public void close() throws IOException {
                server.shutdown();
                query.close();
            }
        };
    }

    private interface ServiceProxy extends Closeable {

        void write(InternalRow row) throws Exception;
    }
}
