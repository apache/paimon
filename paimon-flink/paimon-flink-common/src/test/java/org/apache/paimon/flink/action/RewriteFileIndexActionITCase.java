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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link RewriteFileIndexAction}. */
public class RewriteFileIndexActionITCase extends ActionITCaseBase {
    @Test
    public void testFileIndexAddIndex() throws Exception {

        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse'='%s');",
                        warehouse));
        tEnv.useCatalog("PAIMON");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db;").await();
        tEnv.executeSql("USE test_db").await();

        tEnv.executeSql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '-1'"
                        + ")");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        tEnv.executeSql(
                "INSERT INTO T VALUES (1, '100', 15, '20221208'), (1, '100', 16, '20221208'), (1, '100', 15, '20221209')");
        tEnv.executeSql("ALTER TABLE T SET ('file-index.bloom-filter.columns'='k,v')");

        if (ThreadLocalRandom.current().nextBoolean()) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder()
                            .batchMode()
                            .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                            .build();
            createAction(
                            RewriteFileIndexAction.class,
                            "rewrite_file_index",
                            "--warehouse",
                            warehouse,
                            "--identifier",
                            "test_db.T")
                    .withStreamExecutionEnvironment(env)
                    .run();
        } else {
            executeSQL("CALL sys.rewrite_file_index('test_db.T')", false, true);
        }

        FileStoreTable table = (FileStoreTable) catalog.getTable(new Identifier("test_db", "T"));
        List<ManifestEntry> list = table.store().newScan().plan().files();
        testIndexFile(list, table);
    }

    @Test
    public void testFileIndexAddIndexWithSpecifiedPartition() throws Exception {
        TableEnvironment tEnv = tableEnvironmentBuilder().batchMode().build();
        tEnv.executeSql(
                String.format(
                        "CREATE CATALOG PAIMON WITH ('type'='paimon', 'warehouse'='%s');",
                        warehouse));
        tEnv.useCatalog("PAIMON");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS test_db;").await();
        tEnv.executeSql("USE test_db").await();

        tEnv.executeSql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'bucket' = '-1'"
                        + ")");
        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        tEnv.executeSql(
                "INSERT INTO T VALUES (1, '100', 15, '20221208'), (1, '100', 16, '20221208'), (1, '100', 15, '20221209')");
        tEnv.executeSql("ALTER TABLE T SET ('file-index.bloom-filter.columns'='k,v')");

        if (ThreadLocalRandom.current().nextBoolean()) {
            StreamExecutionEnvironment env =
                    streamExecutionEnvironmentBuilder()
                            .batchMode()
                            .setConf(TableConfigOptions.TABLE_DML_SYNC, true)
                            .build();
            createAction(
                            RewriteFileIndexAction.class,
                            "rewrite_file_index",
                            "--warehouse",
                            warehouse,
                            "--identifier",
                            "test_db.T",
                            "--partitions",
                            "dt=20221208")
                    .withStreamExecutionEnvironment(env)
                    .run();
        } else {
            executeSQL("CALL sys.rewrite_file_index('test_db.T', 'dt=20221208')", false, true);
        }

        FileStoreTable table = (FileStoreTable) catalog.getTable(new Identifier("test_db", "T"));

        List<ManifestEntry> partition20221209List =
                table.store()
                        .newScan()
                        .withPartitionFilter(
                                new PredicateBuilder(table.schema().logicalPartitionType())
                                        .equal(0, BinaryString.fromString("20221209")))
                        .plan()
                        .files();

        partition20221209List.forEach(
                entry -> {
                    List<String> extraFiles =
                            entry.file().extraFiles().stream()
                                    .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                                    .collect(Collectors.toList());

                    // Means no index file
                    assertThat(extraFiles.size()).isEqualTo(0);
                });

        List<ManifestEntry> partition20221208List =
                table.store()
                        .newScan()
                        .withPartitionFilter(
                                new PredicateBuilder(table.schema().logicalPartitionType())
                                        .equal(0, BinaryString.fromString("20221208")))
                        .plan()
                        .files();

        testIndexFile(partition20221208List, table);
    }

    private void testIndexFile(List<ManifestEntry> list, FileStoreTable table) throws Exception {
        for (ManifestEntry entry : list) {
            List<String> extraFiles =
                    entry.file().extraFiles().stream()
                            .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());

            assertThat(extraFiles.size()).isEqualTo(1);

            String file = extraFiles.get(0);

            Path indexFilePath =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toAlignedPath(file, entry.file());
            try (FileIndexFormat.Reader reader =
                    FileIndexFormat.createReader(
                            table.fileIO().newInputStream(indexFilePath), table.rowType())) {
                Set<FileIndexReader> readerSetK = reader.readColumnIndex("k");
                assertThat(readerSetK.size()).isEqualTo(1);

                Predicate predicateK = new PredicateBuilder(table.rowType()).equal(0, 1);
                for (FileIndexReader fileIndexReader : readerSetK) {
                    assertThat(predicateK.visit(fileIndexReader).remain()).isTrue();
                }

                predicateK = new PredicateBuilder(table.rowType()).equal(0, 4);
                for (FileIndexReader fileIndexReader : readerSetK) {
                    assertThat(predicateK.visit(fileIndexReader).remain()).isFalse();
                }

                Set<FileIndexReader> readerSetV = reader.readColumnIndex("v");
                assertThat(readerSetV.size()).isEqualTo(1);

                Predicate predicateV =
                        new PredicateBuilder(table.rowType())
                                .equal(1, BinaryString.fromString("100"));
                for (FileIndexReader fileIndexReader : readerSetV) {
                    assertThat(predicateV.visit(fileIndexReader).remain()).isTrue();
                }

                predicateV =
                        new PredicateBuilder(table.rowType())
                                .equal(1, BinaryString.fromString("101"));
                for (FileIndexReader fileIndexReader : readerSetV) {
                    assertThat(predicateV.visit(fileIndexReader).remain()).isFalse();
                }
            }
        }
    }
}
