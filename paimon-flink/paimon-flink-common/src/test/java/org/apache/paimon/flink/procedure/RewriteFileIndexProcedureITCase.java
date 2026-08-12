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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.table.api.config.TableConfigOptions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/** IT Case for {@link RewriteFileIndexProcedure}. */
public class RewriteFileIndexProcedureITCase extends CatalogITCaseBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFileIndexProcedureSchemaEvolution(boolean isNamedArgument) throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file.format' = 'avro',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql("ALTER TABLE T RENAME `k` TO order_id");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        FileStoreTable table = paimonTable("T");

        Predicate predicateK = new PredicateBuilder(table.rowType()).equal(0, 2);
        Predicate predicateV =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("101"));
        RecordReader<InternalRow> reader =
                table.newRead()
                        .withFilter(PredicateBuilder.and(predicateK, predicateV))
                        .createReader(table.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(r -> count.incrementAndGet());

        // parquet format predicate would not reduce record read from file
        Assertions.assertThat(count.get()).isEqualTo(6);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("ALTER TABLE T SET ('file-index.bloom-filter.columns'='order_id,v')");
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }

        reader =
                table.newRead()
                        .withFilter(PredicateBuilder.and(predicateK, predicateV))
                        .createReader(table.newScan().plan());
        count.set(0);
        reader.forEachRemaining(r -> count.incrementAndGet());

        // the whole file is filtered, none record left
        Assertions.assertThat(count.get()).isEqualTo(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPartitionFilter(boolean isNamedArgument) throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file.format' = 'avro',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        sql(
                "INSERT INTO T VALUES (1, '10', 15, '20221208'), (4, '100', 16, '20221208'), (5, '1000', 15, '20221209')");

        FileStoreTable table = paimonTable("T");

        Predicate predicateK = new PredicateBuilder(table.rowType()).equal(0, 2);
        Predicate predicateV =
                new PredicateBuilder(table.rowType()).equal(1, BinaryString.fromString("101"));
        RecordReader<InternalRow> reader =
                table.newRead()
                        .withFilter(PredicateBuilder.and(predicateK, predicateV))
                        .createReader(table.newScan().plan());
        AtomicInteger count = new AtomicInteger(0);
        reader.forEachRemaining(r -> count.incrementAndGet());

        // parquet format predicate would not reduce record read from file
        Assertions.assertThat(count.get()).isEqualTo(6);

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("ALTER TABLE T SET ('file-index.bloom-filter.columns'='k,v')");
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T', partitions => 'dt=20221208')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T', 'dt=20221208')");
        }

        reader =
                table.newRead()
                        .withFilter(PredicateBuilder.and(predicateK, predicateV))
                        .createReader(table.newScan().plan());
        count.set(0);
        reader.forEachRemaining(r -> count.incrementAndGet());

        // only partition 20221208 is filtered.
        Assertions.assertThat(count.get()).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFileIndexProcedureSwitchIndexType(boolean isNamedArgument) throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file-index.bloom-filter.columns' = 'k',"
                        + " 'file-index.in-manifest-threshold' = '1 MB',"
                        + " 'bucket' = '-1'"
                        + ")");
        sql("INSERT INTO T VALUES (1, '100', '20221208')");

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }
        assertFileIndexTypes("T", "bloom-filter", true);

        sql("ALTER TABLE T SET ('file-index.in-manifest-threshold' = '1 B')");
        sql("ALTER TABLE T RESET ('file-index.bloom-filter.columns')");
        sql("ALTER TABLE T SET ('file-index.bitmap.columns' = 'k')");
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }
        assertFileIndexTypes("T", "bitmap", false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFileIndexProcedureDropEmbeddedIndex(boolean isNamedArgument) throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file-index.bloom-filter.columns' = 'k',"
                        + " 'file-index.in-manifest-threshold' = '1 MB',"
                        + " 'bucket' = '-1'"
                        + ")");
        sql("INSERT INTO T VALUES (1, '20221208')");

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }
        assertFileIndexTypes("T", "bloom-filter", true);

        sql("ALTER TABLE T RESET ('file-index.bloom-filter.columns')");
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }
        assertNoFileIndexes("T");
    }

    private void assertFileIndexTypes(
            String tableName, String expectedIndexType, boolean expectedEmbeddedIndex) throws Exception {
        flinkCatalog()
                .catalog()
                .invalidateTable(Identifier.create(tEnv.getCurrentDatabase(), tableName));
        FileStoreTable table = paimonTable(tableName);
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            byte[] embeddedIndex = entry.file().embeddedIndex();
            FileIndexFormat.Reader reader;
            if (expectedEmbeddedIndex) {
                Assertions.assertThat(embeddedIndex).isNotNull();
                Assertions.assertThat(entry.file().extraFiles())
                        .noneMatch(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX));
                reader =
                        FileIndexFormat.createReader(
                                new ByteArraySeekableStream(embeddedIndex), table.rowType());
            } else {
                Assertions.assertThat(embeddedIndex).isNull();
                String indexFile =
                        entry.file().extraFiles().stream()
                                .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new AssertionError(
                                                        "Missing file index for "
                                                                + entry.file().fileName()));
                Path indexFilePath =
                        table.store()
                                .pathFactory()
                                .createDataFilePathFactory(entry.partition(), entry.bucket())
                                .toAlignedPath(indexFile, entry.file());
                reader =
                        FileIndexFormat.createReader(
                                table.fileIO().newInputStream(indexFilePath), table.rowType());
            }
            try (FileIndexFormat.Reader indexReader = reader) {
                Map<String, Map<String, byte[]>> indexes = indexReader.readAll();
                Assertions.assertThat(indexes).containsKey("k");
                Assertions.assertThat(indexes.get("k").keySet()).containsExactly(expectedIndexType);
            }
        }
    }

    private void assertNoFileIndexes(String tableName) throws Exception {
        flinkCatalog()
                .catalog()
                .invalidateTable(Identifier.create(tEnv.getCurrentDatabase(), tableName));
        FileStoreTable table = paimonTable(tableName);
        for (ManifestEntry entry : table.store().newScan().plan().files()) {
            Assertions.assertThat(entry.file().embeddedIndex()).isNull();
            Assertions.assertThat(entry.file().extraFiles())
                    .noneMatch(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFileIndexProcedureDropIndex(boolean isNamedArgument) throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v STRING,"
                        + " hh INT,"
                        + " dt STRING"
                        + ") PARTITIONED BY (dt, hh) WITH ("
                        + " 'write-only' = 'true',"
                        + " 'file-index.bloom-filter.columns' = 'k,v',"
                        + " 'bucket' = '-1'"
                        + ")");

        sql(
                "INSERT INTO T VALUES (1, '100', 15, '20221208'), (1, '100', 16, '20221208'), (1, '100', 15, '20221209')");

        tEnv.getConfig().set(TableConfigOptions.TABLE_DML_SYNC, true);
        sql("ALTER TABLE T SET ('file-index.bloom-filter.columns'='k')");
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }

        FileStoreTable table = paimonTable("T");
        List<ManifestEntry> list = table.store().newScan().plan().files();

        for (ManifestEntry entry : list) {
            List<String> extraFiles =
                    entry.file().extraFiles().stream()
                            .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());

            Assertions.assertThat(extraFiles.size()).isEqualTo(1);

            String file = extraFiles.get(0);

            Path indexFilePath =
                    table.store()
                            .pathFactory()
                            .createDataFilePathFactory(entry.partition(), entry.bucket())
                            .toAlignedPath(file, entry.file());
            try (FileIndexFormat.Reader reader =
                    FileIndexFormat.createReader(
                            table.fileIO().newInputStream(indexFilePath), table.rowType())) {
                Set<FileIndexReader> readerSetK = reader.readColumnIndex("v");

                Assertions.assertThat(readerSetK.size()).isEqualTo(0);
            }
        }

        sql("ALTER TABLE T RESET ('file-index.bloom-filter.columns')");

        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index('default.T')");
        } else {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        }

        table = paimonTable("T");
        list = table.store().newScan().plan().files();
        for (ManifestEntry entry : list) {
            List<String> extraFiles =
                    entry.file().extraFiles().stream()
                            .filter(s -> s.endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX))
                            .collect(Collectors.toList());

            Assertions.assertThat(extraFiles.size()).isEqualTo(0);
        }

        long latestSnapshotId = table.snapshotManager().latestSnapshotId();
        if (isNamedArgument) {
            sql("CALL sys.rewrite_file_index(`table` => 'default.T')");
        } else {
            sql("CALL sys.rewrite_file_index('default.T')");
        }
        Assertions.assertThat(table.snapshotManager().latestSnapshotId())
                .isEqualTo(latestSnapshotId);
    }
}
