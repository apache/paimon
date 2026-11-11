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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
                        .setString("execution.checkpointing.unaligned.enabled", "false")
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMetadataOperationsWithInvalidOptions(boolean cacheEnabled) throws Exception {
        tEnv = tableEnvironmentBuilder().batchMode().parallelism(1).build();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG metadata WITH ('type'='paimon', 'warehouse'='%s', 'cache-enabled'='%s')",
                        path, cacheEnabled));
        tEnv.useCatalog("metadata");
        tEnv.executeSql(
                "CREATE TABLE legacy (id INT, seq INT, v INT, PRIMARY KEY (id) NOT ENFORCED) "
                        + "WITH ('bucket'='1', 'merge-engine'='partial-update', "
                        + "'changelog-producer'='lookup', 'fields.seq.sequence-group'='v')");
        Catalog catalog = ((FlinkCatalog) tEnv.getCatalog("metadata").get()).catalog();
        Identifier identifier = Identifier.create(DB_NAME, "legacy");
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        FileSystemSchemaManager schemas =
                new FileSystemSchemaManager(table.fileIO(), table.location());
        overwriteOption(table, schemas, "fields.seq.sequence-group", "id,v");
        catalog.invalidateTable(identifier);

        try (CloseableIterator<Row> rows = tEnv.executeSql("SHOW CREATE TABLE legacy").collect()) {
            assertThat(rows.next().getField(0).toString())
                    .contains("fields.seq.sequence-group", "id,v");
        }
        assertThatThrownBy(
                        () -> tEnv.executeSql("SELECT * FROM legacy").await(30, TimeUnit.SECONDS))
                .hasStackTraceContaining("primary key");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql("INSERT INTO legacy VALUES (1, 10, 100)")
                                        .await(30, TimeUnit.SECONDS))
                .hasStackTraceContaining("primary key");
        assertThatThrownBy(() -> tEnv.executeSql("ALTER TABLE legacy SET ('bucket'='1')"))
                .hasStackTraceContaining("primary key");
        assertThat(schemas.listAllIds()).containsExactly(0L);

        tEnv.executeSql("ALTER TABLE legacy SET ('fields.seq.sequence-group'='v')");
        assertThat(schemas.latest().get().id()).isEqualTo(1L);
        tEnv.executeSql("INSERT INTO legacy VALUES (1, 10, 100)").await(30, TimeUnit.SECONDS);
        try (CloseableIterator<Row> rows = tEnv.executeSql("SELECT * FROM legacy").collect()) {
            assertThat(rows.next()).isEqualTo(Row.of(1, 10, 100));
            assertThat(rows.hasNext()).isFalse();
        }

        overwriteOption(table, schemas, "fields.seq.sequence-group", null);
        overwriteOption(table, schemas, "fields.v.aggregate-function", "max");
        catalog.invalidateTable(identifier);
        try (CloseableIterator<Row> rows = tEnv.executeSql("SHOW CREATE TABLE legacy").collect()) {
            assertThat(rows.next().getField(0).toString()).contains("fields.v.aggregate-function");
        }
        assertThatThrownBy(
                        () -> tEnv.executeSql("SELECT * FROM legacy").await(30, TimeUnit.SECONDS))
                .hasStackTraceContaining("Must use sequence group");
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql("INSERT INTO legacy VALUES (1, 10, 100)")
                                        .await(30, TimeUnit.SECONDS))
                .hasStackTraceContaining("Must use sequence group");
        tEnv.executeSql("ALTER TABLE legacy RESET ('fields.v.aggregate-function')");
        tEnv.executeSql("ALTER TABLE legacy SET ('fields.seq.sequence-group'='v')");
        try (CloseableIterator<Row> rows = tEnv.executeSql("SELECT * FROM legacy").collect()) {
            assertThat(rows.next()).isEqualTo(Row.of(1, 10, 100));
            assertThat(rows.hasNext()).isFalse();
        }

        overwriteOption(table, schemas, "full-compaction.delta-commits", "1");
        catalog.invalidateTable(identifier);
        assertThatThrownBy(() -> ((FileStoreTable) catalog.getTable(identifier)).newScan())
                .isInstanceOf(RuntimeException.class);
        tEnv.executeSql("ALTER TABLE legacy RESET ('full-compaction.delta-commits')");
        assertThat(catalog.getTable(identifier).options())
                .doesNotContainKey("full-compaction.delta-commits");

        overwriteOption(table, schemas, "fields.seq.sequence-group", "id,v");
        catalog.invalidateTable(identifier);
        tEnv.executeSql("DROP TABLE legacy");
        assertThat(catalog.listTables(DB_NAME)).doesNotContain("legacy");
        assertThat(table.fileIO().exists(table.location())).isFalse();
    }

    private void overwriteOption(
            FileStoreTable table, FileSystemSchemaManager schemas, String key, String value)
            throws IOException {
        TableSchema schema = schemas.latest().get();
        Map<String, String> options = new HashMap<>(schema.options());
        if (value == null) {
            options.remove(key);
        } else {
            options.put(key, value);
        }
        table.fileIO()
                .overwriteFileUtf8(
                        schemas.toSchemaPath(schema.id()), schema.copy(options).toString());
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
        Path tablePath = new Path(catalog.getTable(identifier).options().get("path"));
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
        assertThat(tableOptions).doesNotContainKey("lock.enabled");

        // check table options override catalog's
        tEnv.executeSql(
                "CREATE TABLE t2_options (a STRING, b STRING, c STRING) WITH ('opt3'='value4')");

        identifier = new Identifier(DB_NAME, "t2_options");
        tableOptions = catalog.getTable(identifier).options();

        assertThat(tableOptions).containsEntry("opt1", "value1");
        assertThat(tableOptions).containsEntry("opt2", "value2");
        assertThat(tableOptions).containsEntry("opt3", "value4");
        assertThat(tableOptions).doesNotContainKey("lock.enabled");
    }

    @Test
    void testCatalogIgnoresBackendLockOptionsForSchema() throws Exception {
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
        assertThat(LOCK_COUNT.get()).isZero();
        assertThat(collect("SHOW TABLES"))
                .containsExactlyInAnyOrder(Row.of("table1"), Row.of("table2"));
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
        public Lock createLock(
                CatalogLockContext context,
                Identifier identifier,
                String tableUuid,
                String commitUser,
                Options tableOptions) {
            return new Lock() {
                @Override
                public AutoCloseable acquire() throws Exception {
                    LOCK_COUNT.incrementAndGet();
                    return () -> {};
                }

                @Override
                public void close() throws IOException {}
            };
        }
    }
}
