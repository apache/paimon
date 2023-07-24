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

package org.apache.paimon.flink.lookup;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/** Tests for {@link FileStoreLookupFunction}. */
public class FileStoreLookupFunctionTest {

    private static final Random RANDOM = new Random();

    private final String commitUser = UUID.randomUUID().toString();
    private final TraceableFileIO fileIO = new TraceableFileIO();
    private FileStoreLookupFunction fileStoreLookupFunction;
    private FileStoreTable fileStoreTable;
    @TempDir private Path tempDir;

    @BeforeEach
    public void before() throws Exception {
        org.apache.paimon.fs.Path path = new org.apache.paimon.fs.Path(tempDir.toString());
        SchemaManager schemaManager = new SchemaManager(fileIO, path);
        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(4096 * 3));
        conf.set(CoreOptions.PAGE_SIZE, new MemorySize(4096));
        conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 3);
        conf.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN, 2);
        conf.set(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL, Duration.ZERO);

        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                        new String[] {"pt", "k", "v"});

        Schema schema =
                new Schema(
                        rowType.getFields(),
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        conf.toMap(),
                        "");
        TableSchema tableSchema = schemaManager.createTable(schema);
        fileStoreTable =
                FileStoreTableFactory.create(
                        fileIO, new org.apache.paimon.fs.Path(tempDir.toString()), tableSchema);

        fileStoreLookupFunction =
                new FileStoreLookupFunction(fileStoreTable, new int[] {0, 1}, new int[] {1}, null);
        fileStoreLookupFunction.open(tempDir.toString());
    }

    @Test
    public void testLookupScanLeak() throws Exception {
        commit(writeCommit(1));
        fileStoreLookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        Assertions.assertEquals(
                TraceableFileIO.openInputStreams(s -> s.toString().contains(tempDir.toString()))
                        .size(),
                0);

        commit(writeCommit(10));
        fileStoreLookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
        Assertions.assertEquals(
                TraceableFileIO.openInputStreams(s -> s.toString().contains(tempDir.toString()))
                        .size(),
                0);
    }

    @Test
    public void testLookupExpiredSnapshot() throws Exception {
        commit(writeCommit(1));
        fileStoreLookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));

        commit(writeCommit(2));
        commit(writeCommit(3));
        commit(writeCommit(4));
        commit(writeCommit(5));
        fileStoreLookupFunction.lookup(new FlinkRowData(GenericRow.of(1, 1, 10L)));
    }

    private void commit(List<CommitMessage> messages) {
        fileStoreTable.newCommit(commitUser).commit(messages);
    }

    private List<CommitMessage> writeCommit(int number) throws Exception {
        List<CommitMessage> messages = new ArrayList<>();
        StreamTableWrite writer = fileStoreTable.newStreamWriteBuilder().newWrite();
        for (int i = 0; i < number; i++) {
            writer.write(randomRow());
            messages.addAll(writer.prepareCommit(true, i));
        }
        return messages;
    }

    private InternalRow randomRow() {
        return GenericRow.of(RANDOM.nextInt(100), RANDOM.nextInt(100), RANDOM.nextLong());
    }
}
