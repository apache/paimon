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

package org.apache.paimon.benchmark.bitmap;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.benchmark.Benchmark;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_LAST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.predicate.SortValue.SortDirection.DESCENDING;

/** Benchmark for table read. */
public class RangeBitmapIndexPushDownBenchmark {

    private static final int VALUE_COUNT = 15;
    private static final int ROW_COUNT = 1000000;
    public static final int[] BOUNDS = new int[] {300, 3000, 30000, 300000, 3000000};

    @TempDir java.nio.file.Path tempFile;

    private final RandomDataGenerator random = new RandomDataGenerator();

    @Test
    public void testParquet() throws Exception {
        for (int bound : BOUNDS) {
            Table table = prepareData(bound, parquet(), "parquet_" + bound);
            Map<String, Table> tables = new LinkedHashMap<>();
            tables.put(
                    "without-index",
                    table.copy(Collections.singletonMap("file-index.read.enabled", "false")));
            tables.put(
                    "with-index",
                    table.copy(Collections.singletonMap("file-index.read.enabled", "true")));

            // benchmark equals
            benchmarkEquals(tables, bound, random.nextInt(0, bound));

            // benchmark between
            benchmarkBetween(tables, bound, 0, 100);

            // benchmark TopN
            benchmarkTopN(tables, bound, 1);
            benchmarkMultipleTopN(tables, bound, 1);
        }
    }

    @Test
    public void testLimitPushDown() throws Exception {
        Random random = new Random();
        for (int bound : BOUNDS) {
            Table table = prepareData(bound, parquet(), "parquet_" + bound);
            Benchmark benchmark =
                    new Benchmark("limit", ROW_COUNT)
                            .setNumWarmupIters(1)
                            .setOutputPerIteration(false);
            int limit = random.nextInt(Math.min(bound, 1000));
            benchmark.addCase(
                    bound + "-" + limit,
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
            benchmark.run();
        }
    }

    private Options parquet() {
        Options options = new Options();
        options.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_PARQUET);
        options.set("file-index.range-bitmap.columns", "k");
        return options;
    }

    private void benchmarkEquals(Map<String, Table> tables, int bound, int value) {
        Benchmark benchmark =
                new Benchmark("equals", ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);
        for (String name : tables.keySet()) {
            benchmark.addCase(
                    name + "-" + bound,
                    5,
                    () -> {
                        Table table = tables.get(name);
                        Predicate predicate = new PredicateBuilder(table.rowType()).equal(0, value);
                        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                        AtomicLong readCount = new AtomicLong(0);
                        try {
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder()
                                                .withFilter(predicate)
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

    private void benchmarkBetween(Map<String, Table> tables, int bound, int from, int to) {
        Benchmark benchmark =
                new Benchmark("between", ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);
        for (String name : tables.keySet()) {
            benchmark.addCase(
                    name + "-" + bound + "-" + from + "-" + to,
                    5,
                    () -> {
                        Table table = tables.get(name);
                        Predicate predicate =
                                new PredicateBuilder(table.rowType()).between(0, from, to);
                        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                        AtomicLong readCount = new AtomicLong(0);
                        try {
                            for (Split split : splits) {
                                RecordReader<InternalRow> reader =
                                        table.newReadBuilder()
                                                .withFilter(predicate)
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

    private void benchmarkTopN(Map<String, Table> tables, int bound, int k) {
        Benchmark benchmark =
                new Benchmark("topn", ROW_COUNT).setNumWarmupIters(1).setOutputPerIteration(false);
        for (String name : tables.keySet()) {
            benchmark.addCase(
                    name + "-" + bound + "-" + k,
                    1,
                    () -> {
                        Table table = tables.get(name);
                        FieldRef ref = new FieldRef(0, "k", DataTypes.INT());
                        TopN topN = new TopN(ref, DESCENDING, NULLS_LAST, k);
                        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
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

    private void benchmarkMultipleTopN(Map<String, Table> tables, int bound, int k) {
        Benchmark benchmark =
                new Benchmark("multiple-topn", ROW_COUNT)
                        .setNumWarmupIters(1)
                        .setOutputPerIteration(false);
        for (String name : tables.keySet()) {
            benchmark.addCase(
                    name + "-" + bound + "-" + k,
                    1,
                    () -> {
                        Table table = tables.get(name);
                        List<SortValue> orders =
                                Arrays.asList(
                                        new SortValue(
                                                new FieldRef(0, "k", DataTypes.INT()),
                                                DESCENDING,
                                                NULLS_LAST),
                                        new SortValue(
                                                new FieldRef(1, "f1", DataTypes.STRING()),
                                                ASCENDING,
                                                NULLS_LAST));
                        TopN topN = new TopN(orders, k);
                        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
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

    private Table prepareData(int bound, Options options, String tableName) throws Exception {
        Table table = createTable(options, tableName);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = writeBuilder.newWrite();
        StreamTableCommit commit = writeBuilder.newCommit();
        AtomicInteger writeCount = new AtomicInteger(0);
        for (int i = 0; i < ROW_COUNT; i++) {
            try {
                write.write(newRandomRow(bound));
                writeCount.incrementAndGet();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        List<CommitMessage> commitMessages = write.prepareCommit(true, 1);
        commit.commit(1, commitMessages);

        write.close();
        return table;
    }

    protected Table createTable(Options tableOptions, String tableName) throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "k", new IntType()));
        for (int i = 1; i <= VALUE_COUNT; i++) {
            fields.add(new DataField(i, "f" + i, DataTypes.STRING()));
        }
        Schema schema =
                new Schema(
                        fields,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        tableOptions.toMap(),
                        "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    protected InternalRow newRandomRow(int bound) {
        GenericRow row = new GenericRow(1 + VALUE_COUNT);
        row.setField(0, random.nextInt(0, bound));
        for (int i = 1; i <= VALUE_COUNT; i++) {
            row.setField(i, BinaryString.fromString(random.nextHexString(32)));
        }
        return row;
    }
}
