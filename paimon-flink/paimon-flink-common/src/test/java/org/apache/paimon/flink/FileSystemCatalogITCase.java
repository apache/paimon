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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@link FlinkCatalog}. */
public class FileSystemCatalogITCase extends AbstractTestBase {
    private static final AtomicInteger LOCK_COUNT = new AtomicInteger(0);

    private static final String DB_NAME = "default";

    private String path;
    private TableEnvironment tEnv;

    @BeforeEach
    public void setup() {
        tEnv =
                tableEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(1)
                        .setConf(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false)
                        .build();
        path = getTempDirPath();
        tEnv.executeSql(
                String.format("CREATE CATALOG fs WITH ('type'='paimon', 'warehouse'='%s')", path));
    }

    @Test
    public void testWriteRead() throws Exception {
        tEnv.useCatalog("fs");
        tEnv.executeSql(
                "CREATE TABLE T (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'a')");
        innerTestWriteRead();
    }

    @Test
    public void testRenameTable() throws Exception {
        tEnv.useCatalog("fs");
        tEnv.executeSql("CREATE TABLE t1 (a INT) WITH ('bucket' = '1', 'bucket-key' = 'a')")
                .await();
        tEnv.executeSql("CREATE TABLE t2 (a INT) WITH ('bucket' = '1', 'bucket-key' = 'a')")
                .await();
        tEnv.executeSql("INSERT INTO t1 VALUES(1),(2)").await();
        // the source table do not exist.
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE t3 RENAME TO t4"))
                .hasMessage("Table `fs`.`default`.`t3` doesn't exist or is a temporary table.");

        // the target table has existed.
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE t1 RENAME TO t2"))
                .hasMessage("Could not execute ALTER TABLE fs.default.t1 RENAME TO fs.default.t2");

        tEnv.executeSql("ALTER TABLE t1 RENAME TO t3").await();
        assertThat(collect("SHOW TABLES")).containsExactlyInAnyOrder(Row.of("t2"), Row.of("t3"));

        Identifier identifier = new Identifier(DB_NAME, "t3");
        Catalog catalog =
                ((FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get()).catalog();
        Path tablePath = ((AbstractCatalog) catalog).getDataTableLocation(identifier);
        assertThat(tablePath.toString())
                .isEqualTo(new File(path, DB_NAME + ".db" + File.separator + "t3").toString());

        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(tEnv.from("t3").execute().collect());
        List<Row> result = iterator.collectAndClose(2);
        assertThat(result).containsExactlyInAnyOrder(Row.of(1), Row.of(2));
    }

    @Test
    public void testCatalogOptionsInheritAndOverride() throws Exception {
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG fs_with_options WITH ("
                                + "'type'='paimon', "
                                + "'warehouse'='%s', "
                                + "'table-default.opt1'='value1', "
                                + "'table-default.opt2'='value2', "
                                + "'table-default.opt3'='value3', "
                                + "'fs.allow-hadoop-fallback'='false',"
                                + "'lock.enabled'='false'"
                                + ")",
                        path));
        tEnv.useCatalog("fs_with_options");

        // check table inherit catalog options
        tEnv.executeSql("CREATE TABLE t1_options (a STRING, b STRING, c STRING)");

        Identifier identifier = new Identifier(DB_NAME, "t1_options");
        Catalog catalog =
                ((FlinkCatalog) tEnv.getCatalog(tEnv.getCurrentCatalog()).get()).catalog();
        Map<String, String> tableOptions = catalog.getTable(identifier).options();

        assertThat(tableOptions).containsEntry("opt1", "value1");
        assertThat(tableOptions).containsEntry("opt2", "value2");
        assertThat(tableOptions).containsEntry("opt3", "value3");
        assertThat(tableOptions).doesNotContainKey("fs.allow-hadoop-fallback");
        assertThat(tableOptions).doesNotContainKey("lock.enabled");

        // check table options override catalog's
        tEnv.executeSql(
                "CREATE TABLE t2_options (a STRING, b STRING, c STRING) WITH ('opt3'='value4')");

        identifier = new Identifier(DB_NAME, "t2_options");
        tableOptions = catalog.getTable(identifier).options();

        assertThat(tableOptions).containsEntry("opt1", "value1");
        assertThat(tableOptions).containsEntry("opt2", "value2");
        assertThat(tableOptions).containsEntry("opt3", "value4");
        assertThat(tableOptions).doesNotContainKey("fs.allow-hadoop-fallback");
        assertThat(tableOptions).doesNotContainKey("lock.enabled");
    }

    @Test
    void testCatalogWithLockForSchema() throws Exception {
        LOCK_COUNT.set(0);
        tEnv.executeSql(
                        String.format(
                                "CREATE CATALOG fs_with_lock WITH ("
                                        + "'type'='paimon', "
                                        + "'warehouse'='%s', "
                                        + "'lock.enabled'='true',"
                                        + "'lock.type'='DUMMY'"
                                        + ")",
                                path))
                .await();
        tEnv.useCatalog("fs_with_lock");
        tEnv.executeSql("CREATE TABLE table1 (a STRING, b STRING, c STRING)").await();
        tEnv.executeSql("CREATE TABLE table2 (a STRING, b STRING, c STRING)").await();
        tEnv.executeSql("CREATE TABLE table3 (a STRING, b STRING, c STRING)").await();
        tEnv.executeSql("DROP TABLE table3").await();
        assertThat(LOCK_COUNT.get()).isEqualTo(3);
    }

    private void innerTestWriteRead() throws Exception {
        tEnv.executeSql("INSERT INTO T VALUES ('1', '2', '3'), ('4', '5', '6')").await();
        BlockingIterator<Row, Row> iterator =
                BlockingIterator.of(tEnv.from("T").execute().collect());
        List<Row> result = iterator.collectAndClose(2);
        assertThat(result).containsExactlyInAnyOrder(Row.of("1", "2", "3"), Row.of("4", "5", "6"));
    }

    private List<Row> collect(String sql) throws Exception {
        List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> it = tEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                result.add(it.next());
            }
        }
        return result;
    }

    /** Lock factory for file system catalog. */
    public static class FileSystemCatalogDummyLockFactory implements CatalogLockFactory {

        private static final String IDENTIFIER = "DUMMY";

        @Override
        public String identifier() {
            return IDENTIFIER;
        }

        @Override
        public CatalogLock createLock(CatalogLockContext context) {
            return new CatalogLock() {
                @Override
                public <T> T runWithLock(String database, String table, Callable<T> callable)
                        throws Exception {
                    LOCK_COUNT.incrementAndGet();
                    return callable.call();
                }

                @Override
                public void close() throws IOException {}
            };
        }
    }
}
