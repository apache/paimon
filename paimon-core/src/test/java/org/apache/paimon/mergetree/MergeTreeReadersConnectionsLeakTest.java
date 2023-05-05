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

package org.apache.paimon.mergetree;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.FailingConstructInputOutputIO;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.apache.paimon.CoreOptions.NUM_SORTED_RUNS_COMPACTION_TRIGGER;

/** Test for {@link MergeTreeReaders} check the reader close state when an io exception happens. */
public class MergeTreeReadersConnectionsLeakTest {
    @TempDir java.nio.file.Path tempDir;

    String dabaseName = "my_db";
    String tableName = "my_table";

    @RepeatedTest(20)
    public void testFailedStream() {
        FailingConstructInputOutputIO fileIO =
                new FailingConstructInputOutputIO(0.3, MergeTreeReaders.class);
        try {
            Path warehouse = new Path(tempDir.toString());

            Catalog catalog = new FileSystemCatalog(fileIO, warehouse);
            createTable(catalog);
            Table table = getTable(catalog);
            writeData(table, 40);
            readData(table);
        } catch (Exception e) {
            // ignore
        }

        Assertions.assertThat(fileIO.noLeak()).isTrue();
    }

    private void createTable(Catalog catalog) throws Exception {
        catalog.createDatabase(dabaseName, true);
        catalog.createTable(new Identifier(dabaseName, tableName), schema(), true);
    }

    private Table getTable(Catalog catalog) throws Exception {
        return catalog.getTable(new Identifier(dabaseName, tableName));
    }

    private Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.STRING());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.primaryKey("f0", "f1", "f2");
        schemaBuilder.partitionKeys("f0");
        schemaBuilder.option("bucket", "40");
        schemaBuilder.option("bucket-key", "f1");
        // no compaction for this test, we need to read more files to construct a reader list
        schemaBuilder.option(NUM_SORTED_RUNS_COMPACTION_TRIGGER.key(), "1000");
        return schemaBuilder.build();
    }

    private void writeData(Table table, int size) throws Exception {
        for (int i = 0; i < 5; i++) {
            // write five times
            writeOnce(table, size);
        }
    }

    private void writeOnce(Table table, int size) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        BatchTableWrite batchTableWrite = builder.newWrite();

        for (int i = 0; i < size; i++) {
            batchTableWrite.write(
                    GenericRow.of(
                            0,
                            BinaryString.fromString(String.valueOf(0)),
                            BinaryString.fromString("aaaaaaaaaaaaa" + i),
                            BinaryString.fromString("b")));
        }

        List<CommitMessage> messList = batchTableWrite.prepareCommit();
        BatchTableCommit commit = builder.newCommit();
        commit.commit(messList);
        commit.close();
    }

    private void readData(Table table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        TableScan.Plan plan = builder.newScan().plan();
        RecordReader<InternalRow> reader = builder.newRead().createReader(plan);
        reader.forEachRemaining(d -> {});
    }
}
