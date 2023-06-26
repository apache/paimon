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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for table. */
public abstract class TableTestBase {

    @TempDir java.nio.file.Path tempPath;

    protected Path warehouse;
    protected Catalog catalog;
    protected String database;

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
        catalog = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        catalog.createDatabase(database, true);
    }

    @AfterEach
    public void after() throws IOException {
        // assert all connections are closed
        Predicate<Path> pathPredicate = path -> path.toString().contains(tempPath.toString());
        assertThat(TraceableFileIO.openInputStreams(pathPredicate)).isEmpty();
        assertThat(TraceableFileIO.openOutputStreams(pathPredicate)).isEmpty();
    }

    protected Identifier identifier(String tableName) {
        return new Identifier(database, tableName);
    }

    protected void write(Table table, InternalRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (InternalRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    protected void compact(Table table, BinaryRow partition, int bucket) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.compact(partition, bucket, true);
            commit.commit(write.prepareCommit());
        }
    }

    protected List<InternalRow> read(Table table, Pair<ConfigOption<?>, String>... dynamicOptions)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        table = table.copy(options);
        ReadBuilder readBuilder = table.newReadBuilder();
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }
}
