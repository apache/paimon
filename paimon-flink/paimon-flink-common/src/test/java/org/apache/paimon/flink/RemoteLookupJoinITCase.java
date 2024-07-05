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
import org.apache.paimon.flink.query.RemoteTableQuery;
import org.apache.paimon.flink.service.QueryService;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.service.network.stats.DisabledServiceRequestStats;
import org.apache.paimon.service.server.KvQueryServer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
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
    public void testQueryServiceLookup() throws Exception {
        sql(
                "CREATE TABLE DIM (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ('bucket' = '2', 'continuous.discovery-interval' = '1ms')");
        CloseableIterator<Row> service = streamSqlIter("CALL sys.query_service('default.DIM', 2)");
        RemoteTableQuery query = new RemoteTableQuery(paimonTable("DIM"));

        sql("INSERT INTO DIM VALUES (1, 11), (2, 22), (3, 33), (4, 44)");
        Thread.sleep(2000);

        assertThat(query.lookup(row(), 0, row(1)))
                .isNotNull()
                .extracting(r -> r.getInt(1))
                .isEqualTo(11);
        assertThat(query.lookup(row(), 0, row(2)))
                .isNotNull()
                .extracting(r -> r.getInt(1))
                .isEqualTo(22);
        assertThat(query.lookup(row(), 1, row(3)))
                .isNotNull()
                .extracting(r -> r.getInt(1))
                .isEqualTo(33);
        assertThat(query.lookup(row(), 0, row(4)))
                .isNotNull()
                .extracting(r -> r.getInt(1))
                .isEqualTo(44);
        assertThat(query.lookup(row(), 0, row(5))).isNull();

        service.close();
        query.cancel().get();
    }

    @Test
    public void testLookupRemoteTable() throws Throwable {
        sql(
                "CREATE TABLE DIM (i INT PRIMARY KEY NOT ENFORCED, j INT, k1 INT, k2 INT) WITH ('bucket' = '1')");
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

    @Test
    public void testServiceFileCleaned() throws Exception {
        sql(
                "CREATE TABLE DIM (k INT PRIMARY KEY NOT ENFORCED, v INT) WITH ('bucket' = '2', 'continuous.discovery-interval' = '1ms')");
        JobClient client = queryService(paimonTable("DIM"));

        RemoteTableQuery query = new RemoteTableQuery(paimonTable("DIM"));

        sql("INSERT INTO DIM VALUES (1, 11), (2, 22), (3, 33), (4, 44)");
        Thread.sleep(2000);

        assertThat(query.lookup(row(), 0, row(1)))
                .isNotNull()
                .extracting(r -> r.getInt(1))
                .isEqualTo(11);

        client.cancel().get();
        query.cancel().get();
        ServiceManager serviceManager = paimonTable("DIM").store().newServiceManager();
        assertThat(serviceManager.service(PRIMARY_KEY_LOOKUP).isPresent()).isFalse();
    }

    private JobClient queryService(FileStoreTable table) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        QueryService.build(env, table, 2);
        return env.executeAsync();
    }

    private ServiceProxy launchQueryServer(String tableName) throws Throwable {
        FileStoreTable table = (FileStoreTable) paimonTable(tableName);
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

        InetSocketAddress[] addresses = new InetSocketAddress[] {server.getServerAddress()};
        ServiceManager serviceManager = table.store().newServiceManager();
        serviceManager.resetService(PRIMARY_KEY_LOOKUP, addresses);

        return new ServiceProxy() {

            @Override
            public void write(InternalRow row) throws Exception {
                BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit();
                write.write(row);
                List<CommitMessage> commitMessages = write.prepareCommit();
                commit.commit(commitMessages);

                CommitMessageImpl message = (CommitMessageImpl) commitMessages.get(0);
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
