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

package org.apache.paimon.compatibility;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
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
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReleaseTableCompatibility {
    public static void main(String[] args) throws Exception {
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(args[0])));
        String stage = args[1];
        if ("createBlob".equals(stage)) {
            DataType blob = (DataType) DataTypes.class.getMethod("BLOB").invoke(null);
            catalog.createTable(
                    Identifier.create("db", "new_feature"),
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("image", blob)
                            .option("data-evolution.enabled", "true")
                            .option("row-tracking.enabled", "true")
                            .option("bucket", "-1")
                            .build(),
                    false);
            System.out.println("created BLOB table");
            return;
        }
        if ("readBlob".equals(stage)) {
            catalog.getTable(Identifier.create("db", "new_feature"));
            System.out.println("opened BLOB table");
            return;
        }
        if ("create".equals(stage)) {
            catalog.createDatabase("db", false);
            for (String name : new String[] {"append", "pk"}) {
                Schema.Builder builder =
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("value", DataTypes.STRING())
                                .option("bucket", "1")
                                .option("file.format", "parquet");
                if ("pk".equals(name)) {
                    builder.primaryKey("id");
                } else {
                    builder.option("bucket-key", "id");
                }
                catalog.createTable(Identifier.create("db", name), builder.build(), false);
                write(catalog.getTable(Identifier.create("db", name)), 1, "one");
            }
        } else if ("write2".equals(stage)) {
            write(catalog.getTable(Identifier.create("db", "append")), 2, "two");
            write(catalog.getTable(Identifier.create("db", "pk")), 1, "updated");
            write(catalog.getTable(Identifier.create("db", "pk")), 2, "two");
        } else if ("write3".equals(stage)) {
            write(catalog.getTable(Identifier.create("db", "append")), 3, "three");
            write(catalog.getTable(Identifier.create("db", "pk")), 3, "three");
        }
        for (String name : new String[] {"append", "pk"}) {
            List<String> rows = read(catalog.getTable(Identifier.create("db", name)));
            System.out.println(stage + " " + name + " " + rows);
            List<String> expected = new ArrayList<>();
            boolean initial = "create".equals(stage) || "read1".equals(stage);
            expected.add("1:" + (initial ? "one" : "pk".equals(name) ? "updated" : "one"));
            if (!initial) {
                expected.add("2:two");
            }
            if ("write3".equals(stage) || "read3".equals(stage)) {
                expected.add("3:three");
            }
            if (!rows.equals(expected)) {
                throw new AssertionError(name + " expected " + expected + " but got " + rows);
            }
        }
    }

    private static void write(Table table, int id, String value) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite writer = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            writer.write(GenericRow.of(id, BinaryString.fromString(value)));
            commit.commit(writer.prepareCommit());
        }
    }

    private static List<String> read(Table table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<String> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                     builder.newRead().createReader(builder.newScan().plan().splits())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getString(1)));
        }
        Collections.sort(rows);
        return rows;
    }
}
