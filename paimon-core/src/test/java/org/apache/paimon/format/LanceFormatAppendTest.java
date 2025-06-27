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

package org.apache.paimon.format;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Test base for append. */
public class LanceFormatAppendTest {

    protected static final Random RANDOM = new Random();

    protected Catalog catalog;
    protected String tableName = "Orders1";
    protected String dataBaseName = "my_db";

    @TempDir Path dir;

    @Test
    public void testFormat() throws Exception {
        createTable();

        long t1 = System.currentTimeMillis();
        Table table = catalog.getTable(identifier());
        List<CommitMessage> messages = writeOnce(table, 1, 10000);
        System.out.println("Write cost: " + (System.currentTimeMillis() - t1) + "ms");

        table.newBatchWriteBuilder().newCommit().commit(messages);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().plan();

        Assertions.assertThatCode(
                        () -> {
                            for (int i = 0; i < 1; i++) {
                                RecordReader<InternalRow> rows =
                                        readBuilder.newRead().createReader(plan);
                                rows.forEachRemaining(s -> {});
                            }
                        })
                .doesNotThrowAnyException();
    }

    protected List<CommitMessage> writeOnce(Table table, int size) throws Exception {
        return writeOnce(table, 10, size);
    }

    protected List<CommitMessage> writeOnce(Table table, int times, int size) throws Exception {
        List<CommitMessage> list = new ArrayList<>();

        StreamWriteBuilder builder = table.newStreamWriteBuilder();
        StreamTableWrite batchTableWrite = builder.newWrite();

        for (int j = 0; j < times; j++) {
            for (int i = 0; i < size; i++) {
                batchTableWrite.write(dataOne());
            }

            list.addAll(batchTableWrite.prepareCommit(true, 0));
            batchTableWrite = builder.newWrite();
        }
        return list;
    }

    protected Catalog getCatalog() {
        if (catalog == null) {
            Options options = new Options();

            options.set(
                    CatalogOptions.WAREHOUSE,
                    new org.apache.paimon.fs.Path(dir.toString()).toUri().toString());
            catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        }
        return catalog;
    }

    protected void createTable() throws Exception {
        getCatalog().dropTable(identifier(), true);
        getCatalog().createDatabase(dataBaseName, true);
        getCatalog().createTable(identifier(), schema(), true);
    }

    protected Identifier identifier() {
        return Identifier.create(dataBaseName, tableName);
    }

    protected InternalRow dataOne() {
        return GenericRow.of(
                BinaryString.fromString("a"),
                BinaryString.fromString("b"),
                BinaryString.fromString(randomString(1024)),
                BinaryString.fromString(randomString(1024)));
    }

    private String randomString(int length) {
        int bound = 'z' - '0';
        char[] b = new char[length];
        for (int i = 0; i < length; i++) {
            b[i] = (char) ('0' + RANDOM.nextInt(bound));
        }
        return new String(b);
    }

    protected Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.STRING());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("scan.parallelism", "5");
        schemaBuilder.option("sink.parallelism", "5");
        schemaBuilder.option("file.format", format());
        schemaBuilder.option("async-file-write", "false");
        schemaBuilder.partitionKeys("f0", "f1");
        return schemaBuilder.build();
    }

    protected String format() {
        return "lance";
    }
}
