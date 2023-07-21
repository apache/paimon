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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/** */
public class ZorderRewriteActionITCase extends ActionITCaseBase {

    @TempDir private java.nio.file.Path path;
    private Catalog catalog;
    private Random random = new Random();

    private void prepareData(int size, int loop) throws Exception {
        createTable();
        List<CommitMessage> commitMessages = new ArrayList<>();
        for (int i = 0; i < loop; i++) {
            commitMessages.addAll(writeData(size));
        }
        commit(commitMessages);
    }

    @Test
    public void testAllBasicTypeWorksWithZorder() throws Exception {
        prepareData(300, 1);

        // All the basic types should support zorder
        Assertions.assertThatCode(
                        () ->
                                zoder(
                                        Arrays.asList(
                                                "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7",
                                                "f8", "f9", "f10", "f11", "f12", "f13", "f14",
                                                "f15")))
                .doesNotThrowAnyException();
    }

    @Test
    public void testActionWorks() throws Exception {
        prepareData(300, 30);

        PredicateBuilder predicateBuilder = new PredicateBuilder(getTable().rowType());
        Predicate predicate = predicateBuilder.between(1, 100, 200);

        List<ManifestEntry> files =
                ((AppendOnlyFileStoreTable) getTable()).store().newScan().plan().files();
        List<ManifestEntry> filesFilter =
                ((AppendOnlyFileStoreTable) getTable())
                        .store()
                        .newScan()
                        .withFilter(predicate)
                        .plan()
                        .files();
        // before zorder, we don't filter any file
        Assertions.assertThat(files.size()).isEqualTo(filesFilter.size());

        new ZorderRewriteAction(
                        new Path(path.toUri()).toUri().toString(),
                        "my_db",
                        "Orders1",
                        "SELECT * FROM my_db.Orders1",
                        Collections.emptyMap(),
                        Arrays.asList("f1", "f2"))
                .run();

        files = ((AppendOnlyFileStoreTable) getTable()).store().newScan().plan().files();
        filesFilter =
                ((AppendOnlyFileStoreTable) getTable())
                        .store()
                        .newScan()
                        .withFilter(predicate)
                        .plan()
                        .files();

        Assertions.assertThat(files.size()).isGreaterThan(filesFilter.size());
    }

    private void zoder(List<String> columns) throws Exception {
        new ZorderRewriteAction(
                        new Path(path.toUri()).toUri().toString(),
                        "my_db",
                        "Orders1",
                        "SELECT * FROM my_db.Orders1",
                        Collections.emptyMap(),
                        columns)
                .run();
    }

    public Catalog getCatalog() {
        if (catalog == null) {
            Options options = new Options();
            options.set(CatalogOptions.WAREHOUSE, new Path(path.toUri()).toUri().toString());
            catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        }
        return catalog;
    }

    public void createTable() throws Exception {
        getCatalog().createDatabase("my_db", true);
        getCatalog().createTable(identifier(), schema(), true);
    }

    public Identifier identifier() {
        return Identifier.create("my_db", "Orders1");
    }

    private void commit(List<CommitMessage> messages) throws Exception {
        getTable().newBatchWriteBuilder().newCommit().commit(messages);
    }

    // schema with all the basic types.
    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.TINYINT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.column("f4", DataTypes.DOUBLE());
        schemaBuilder.column("f5", DataTypes.CHAR(10));
        schemaBuilder.column("f6", DataTypes.VARCHAR(10));
        schemaBuilder.column("f7", DataTypes.BOOLEAN());
        schemaBuilder.column("f8", DataTypes.DATE());
        schemaBuilder.column("f9", DataTypes.TIME());
        schemaBuilder.column("f10", DataTypes.TIMESTAMP());
        schemaBuilder.column("f11", DataTypes.DECIMAL(10, 2));
        schemaBuilder.column("f12", DataTypes.BYTES());
        schemaBuilder.column("f13", DataTypes.FLOAT());
        schemaBuilder.column("f14", DataTypes.BINARY(10));
        schemaBuilder.column("f15", DataTypes.VARBINARY(10));
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("scan.parallelism", "6");
        schemaBuilder.option("sink.parallelism", "3");
        schemaBuilder.option("target-file-size", "1 M");
        schemaBuilder.partitionKeys("f0");
        return schemaBuilder.build();
    }

    private List<CommitMessage> writeData(int size) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            messages.addAll(writeOnce(getTable(), i, size));
        }

        return messages;
    }

    public Table getTable() throws Exception {
        return getCatalog().getTable(identifier());
    }

    private List<CommitMessage> writeOnce(Table table, int p, int size) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < size; j++) {
                    batchTableWrite.write(data(p, i, j));
                }
            }
            return batchTableWrite.prepareCommit();
        }
    }

    private InternalRow data(int p, int i, int j) {
        return GenericRow.of(
                (byte) p,
                j,
                (short) i,
                BinaryString.fromString(String.valueOf(j)),
                0.1 + i,
                BinaryString.fromString(String.valueOf(j)),
                BinaryString.fromString(String.valueOf(i)),
                j % 2 == 1,
                i,
                j,
                Timestamp.fromEpochMillis(i),
                Decimal.zero(10, 2),
                String.valueOf(i).getBytes(),
                (float) 0.1 + j,
                randomBytes(),
                randomBytes());
    }

    private byte[] randomBytes() {
        byte[] binary = new byte[random.nextInt(10)];
        random.nextBytes(binary);
        return binary;
    }
}
