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

package org.apache.paimon.benchmark.dv;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;

/** Benchmark reading for the primary-key table in the deletion-vector. */
public class DeletionVectorPushDownBenchmark {

    private static final int VALUE_COUNT = 15;
    private static final int ROW_COUNT = 1500000;
    private static final int UPDATED_ROW_COUNT = 500000;

    @TempDir java.nio.file.Path tempFile;

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Test
    public void test() throws Exception {
        Table table = prepareData(new Options(), "test01");
        Map<String, Table> tables = new LinkedHashMap<>();
        tables.put(
                "pushdown-disable",
                table.copy(Collections.singletonMap("file-index.read.enabled", "false")));
        tables.put(
                "pushdown-enable",
                table.copy(Collections.singletonMap("file-index.read.enabled", "true")));

        // benchmark limit
        benchmarkLimit(tables, 1);

        // benchmark TopN with primary key
        benchmarkTopN(tables, 1);
    }

    public void benchmarkLimit(Map<String, Table> tables, int limit) {
        Benchmark benchmark =
                new Benchmark("limit", ROW_COUNT).setNumWarmupIters(1).setOutputPerIteration(false);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            String key = entry.getKey();
            Table table = entry.getValue();
            benchmark.addCase(
                    key + "-" + limit,
                    1,
                    () -> {
                        List<Split> splits =
                                table.newReadBuilder().withLimit(limit).newScan().plan().splits();
                        AtomicLong readCount = new AtomicLong(0);
                        try {
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder()
                                                .withLimit(limit)
                                                .newRead()
                                                .createReader(split);
                                reader.forEachRemaining(row -> readCount.incrementAndGet());
                                reader.close();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        benchmark.run();
    }

    public void benchmarkTopN(Map<String, Table> tables, int limit) {
        Benchmark benchmark =
                new Benchmark("TopN", ROW_COUNT).setNumWarmupIters(1).setOutputPerIteration(false);
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            String key = entry.getKey();
            Table table = entry.getValue();
            benchmark.addCase(
                    key + "-" + limit,
                    1,
                    () -> {
                        FieldRef ref = new FieldRef(0, "id", DataTypes.INT());
                        TopN topN = new TopN(ref, DESCENDING, NULLS_FIRST, limit);
                        List<Split> splits =
                                table.newReadBuilder().withTopN(topN).newScan().plan().splits();
                        AtomicLong readCount = new AtomicLong(0);
                        try {
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder()
                                                .withTopN(topN)
                                                .newRead()
                                                .createReader(split);
                                reader.forEachRemaining(row -> readCount.incrementAndGet());
                                reader.close();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        benchmark.run();
    }

    private Table prepareData(Options options, String tableName) throws Exception {
        FileStoreTable table = createTable(options, tableName);
        StreamTableWrite write =
                table.newWrite("user").withIOManager(new IOManagerImpl(tempFile.toString()));
        StreamTableCommit commit = table.newCommit("user");
        for (int i = 0; i < ROW_COUNT; i++) {
            try {
                write.write(newRandomRow(i));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        commit.commit(1, write.prepareCommit(true, 1));

        for (int i = 0; i < UPDATED_ROW_COUNT; i++) {
            try {
                int id = random.nextInt(0, ROW_COUNT);
                write.write(newRandomRow(id));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();
        return table;
    }

    protected FileStoreTable createTable(Options tableOptions, String tableName) throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        tableOptions.set(CoreOptions.BUCKET, 3);
        tableOptions.set(CoreOptions.DELETION_VECTORS_ENABLED, true);
        tableOptions.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);

        List<DataField> fields = new ArrayList<>();
        DataField primaryKey = new DataField(0, "id", new IntType());
        fields.add(primaryKey);
        for (int i = 1; i <= VALUE_COUNT; i++) {
            fields.add(new DataField(i, "f" + i, DataTypes.STRING()));
        }
        Schema schema =
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.singletonList(primaryKey.name()),
                        tableOptions.toMap(),
                        "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    protected InternalRow newRandomRow(int id) {
        GenericRow row = new GenericRow(1 + VALUE_COUNT);
        row.setField(0, id);
        for (int i = 1; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(32)));
        }
        return row;
    }
}
