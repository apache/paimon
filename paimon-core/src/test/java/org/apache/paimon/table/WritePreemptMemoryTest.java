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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrimaryKeyFileStoreTable}. */
public class WritePreemptMemoryTest extends FileStoreTableTestBase {

    @Test
    public void writeMultiplePartitions() throws Exception {
        testWritePreemptMemory(false);
    }

    @Test
    public void writeSinglePartition() throws Exception {
        testWritePreemptMemory(true);
    }

    @Override // this has been tested in PrimaryKeyFileStoreTableTest
    @Test
    public void testReadFilter() {}

    private void testWritePreemptMemory(boolean singlePartition) throws Exception {
        // write
        FileStoreTable table = createFileStoreTable();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);
        Random random = new Random();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            GenericRow row = rowData(singlePartition ? 0 : random.nextInt(5), i, i * 10L);
            write.write(row);
            expected.add(BATCH_ROW_TO_STRING.apply(row));
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        // read
        List<Split> splits = toSplits(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        List<String> results = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            results.addAll(getResult(read, splits, binaryRow(i), 0, BATCH_ROW_TO_STRING));
        }
        assertThat(results).containsExactlyInAnyOrder(expected.toArray(new String[0]));
    }

    @Override
    protected FileStoreTable createFileStoreTable(Consumer<Options> configure, RowType rowType)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.TABLE_SCHEMA_PATH, tablePath.toString());
        // Run with minimal memory to ensure a more intense preempt
        // Currently a writer needs at least one page
        int pages = 10;
        options.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(pages * 1024));
        options.set(CoreOptions.PAGE_SIZE, new MemorySize(1024));
        configure.accept(options);
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                rowType.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                options.toMap(),
                                ""));
        return new PrimaryKeyFileStoreTable(FileIOFinder.find(tablePath), tablePath, schema);
    }

    @Override
    protected FileStoreTable createFileStoreTable(String branch, Consumer<Options> configure)
            throws Exception {
        Options options = new Options();
        options.set(CoreOptions.BUCKET, 1);
        options.set(CoreOptions.TABLE_SCHEMA_PATH, tablePath.toString());
        // Run with minimal memory to ensure a more intense preempt
        // Currently a writer needs at least one page
        int pages = 10;
        options.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(pages * 1024));
        options.set(CoreOptions.PAGE_SIZE, new MemorySize(1024));
        options.set(CoreOptions.BRANCH, branch);
        configure.accept(options);
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath, branch),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "a"),
                                options.toMap(),
                                ""));
        return new PrimaryKeyFileStoreTable(FileIOFinder.find(tablePath), tablePath, schema);
    }

    @Override
    protected FileStoreTable overwriteTestFileStoreTable() throws Exception {
        Options conf = new Options();
        conf.set(CoreOptions.TABLE_SCHEMA_PATH, tablePath.toString());
        conf.set(CoreOptions.BUCKET, 1);
        // Run with minimal memory to ensure a more intense preempt
        // Currently a writer needs at least one page
        int pages = 10;
        conf.set(CoreOptions.WRITE_BUFFER_SIZE, new MemorySize(pages * 1024));
        conf.set(CoreOptions.PAGE_SIZE, new MemorySize(1024));
        TableSchema schema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        new Schema(
                                OVERWRITE_TEST_ROW_TYPE.getFields(),
                                Arrays.asList("pt0", "pt1"),
                                Arrays.asList("pk", "pt0", "pt1"),
                                conf.toMap(),
                                ""));
        return new PrimaryKeyFileStoreTable(FileIOFinder.find(tablePath), tablePath, schema);
    }
}
