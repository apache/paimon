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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Projection;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base for table. */
public abstract class TableTestBase {

    protected static final Random RANDOM = new Random();
    protected static final String DEFAULT_TABLE_NAME = "MyTable";

    protected final String commitUser = UUID.randomUUID().toString();

    protected Path warehouse;
    protected Catalog catalog;
    protected String database;
    @TempDir public java.nio.file.Path tempPath;

    @BeforeEach
    public void beforeEach() throws Catalog.DatabaseAlreadyExistException {
        database = "default";
        if (System.getProperty("os.name").startsWith("Windows")
                && Pattern.compile("^/?[a-zA-Z]:").matcher(tempPath.toString()).find()) {
            warehouse = new Path(tempPath.toString());
        } else {
            warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempPath.toString());
        }
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

    protected Identifier identifier() {
        return identifier(DEFAULT_TABLE_NAME);
    }

    @SafeVarargs
    protected final void write(Table table, Pair<InternalRow, Integer>... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            for (Pair<InternalRow, Integer> row : rows) {
                write.write(row.getKey(), row.getValue());
            }
            commit.commit(write.prepareCommit());
        }
    }

    protected void write(Table table, InternalRow... rows) throws Exception {
        write(table, null, rows);
    }

    protected void write(Table table, IOManager ioManager, InternalRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
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
        return read(table, null, dynamicOptions);
    }

    protected List<InternalRow> read(
            Table table,
            @Nullable int[][] projection,
            Pair<ConfigOption<?>, String>... dynamicOptions)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        for (Pair<ConfigOption<?>, String> pair : dynamicOptions) {
            options.put(pair.getKey().key(), pair.getValue());
        }
        table = table.copy(options);
        ReadBuilder readBuilder = table.newReadBuilder();
        if (projection != null) {
            readBuilder.withProjection(projection);
        }
        RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan());
        InternalRowSerializer serializer =
                new InternalRowSerializer(
                        projection == null
                                ? table.rowType()
                                : Projection.of(projection).project(table.rowType()));
        List<InternalRow> rows = new ArrayList<>();
        reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        return rows;
    }

    public void createTableDefault() throws Exception {
        catalog.createTable(identifier(), schemaDefault(), true);
    }

    public void createTable(Identifier identifier) throws Exception {
        catalog.createTable(identifier, schemaDefault(), false);
    }

    protected void commitDefault(List<CommitMessage> messages) throws Exception {
        BatchTableCommit commit = getTableDefault().newBatchWriteBuilder().newCommit();
        commit.commit(messages);
        commit.close();
    }

    protected List<CommitMessage> writeDataDefault(int size, int times) throws Exception {
        return writeData(getTableDefault(), size, times);
    }

    protected List<CommitMessage> writeData(Table table, int size, int times) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            messages.addAll(writeOnce(table, i, size));
        }
        return messages;
    }

    public FileStoreTable getTableDefault() throws Exception {
        return getTable(identifier());
    }

    public FileStoreTable getTable(Identifier identifier) throws Exception {
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private List<CommitMessage> writeOnce(Table table, int time, int size) throws Exception {
        StreamWriteBuilder builder = table.newStreamWriteBuilder();
        builder.withCommitUser(commitUser);
        try (StreamTableWrite streamTableWrite = builder.newWrite()) {
            for (int j = 0; j < size; j++) {
                streamTableWrite.write(dataDefault(time, j));
            }
            return streamTableWrite.prepareCommit(false, Long.MAX_VALUE);
        }
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BYTES());
        return schemaBuilder.build();
    }

    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(RANDOM.nextInt(), randomString(), randomBytes());
    }

    protected BinaryString randomString() {
        int length = RANDOM.nextInt(50);
        byte[] buffer = new byte[length];

        for (int i = 0; i < length; i += 1) {
            buffer[i] = (byte) ('a' + RANDOM.nextInt(26));
        }

        return BinaryString.fromBytes(buffer);
    }

    protected byte[] randomBytes() {
        byte[] binary = new byte[RANDOM.nextInt(10)];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
