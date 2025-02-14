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

package org.apache.paimon.table;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Test partial update table. */
public class PartialUpdateTableTest {

    @TempDir public static java.nio.file.Path tempDir;
    private Catalog catalog;
    private final Identifier identifier = Identifier.create("my_db", "my_table");

    @BeforeEach
    public void before() throws Exception {
        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, new Path(path()).toUri().toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase("my_db", true);
        catalog.createTable(identifier, schema(), true);
    }

    private String path() {
        return tempDir.toString() + "/" + PartialUpdateTableTest.class.getSimpleName();
    }

    private static Schema schema() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("biz_no", DataTypes.INT());
        schemaBuilder.column("customer_id", DataTypes.STRING());
        schemaBuilder.column("payable_amount", DataTypes.INT());
        schemaBuilder.column("g1", DataTypes.INT());
        schemaBuilder.primaryKey("biz_no");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("file.format", "parquet");
        schemaBuilder.option("merge-engine", "partial-update");
        schemaBuilder.option("fields.g1.sequence-group", "payable_amount");
        schemaBuilder.option("fields.payable_amount.aggregation-function", "sum");
        schemaBuilder.option("deletion-vectors.enabled", "true");
        schemaBuilder.option("write-buffer-spillable", "true");
        return schemaBuilder.build();
    }

    @Test
    public void testWriteDeleteRecordWithNoInsertData() throws Exception {
        Table table = catalog.getTable(identifier);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (StreamTableCommit commit = writeBuilder.newCommit();
                StreamTableWrite write = writeBuilder.newWrite()) {
            write.withIOManager(new IOManagerImpl(tempDir.toString()));
            for (int snapshotId = 0; snapshotId < 100; snapshotId++) {
                int bizNo = snapshotId;
                String customerId = String.valueOf(snapshotId);
                int payableAmount = 1;
                int g1 = 1;
                write.write(
                        GenericRow.ofKind(
                                snapshotId == 0 || snapshotId == 10
                                        ? RowKind.DELETE
                                        : RowKind.INSERT,
                                bizNo,
                                BinaryString.fromString(customerId),
                                payableAmount,
                                g1));
                commit.commit(snapshotId, write.prepareCommit(true, snapshotId));
            }
        }

        ReadBuilder builder = table.newReadBuilder();
        TableScan scan = builder.newScan();
        TableScan.Plan plan = scan.plan();

        AtomicInteger i = new AtomicInteger(0);
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(plan)) {
            reader.forEachRemaining(
                    row -> {
                        if (i.get() == 0 || i.get() == 10) {
                            i.incrementAndGet();
                        }
                        int index = i.get();
                        assertThat(row.getInt(0)).isEqualTo(index);
                        assertThat(row.getString(1).toString()).isEqualTo(String.valueOf(index));
                        assertThat(row.getInt(2)).isEqualTo(1);
                        assertThat(row.getInt(3)).isEqualTo(1);
                        i.incrementAndGet();
                    });
        }
    }
}
