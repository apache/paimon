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

package org.apache.paimon.operation;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AbstractFileStoreWrite#close()}. */
public class AbstractFileStoreWriteCloseTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCloseReleasesEveryWriterWhenAnEarlierOneThrows() throws Exception {
        BucketedAppendFileStoreWrite write = newWrite();

        // Three writers in one partition. The first throws, so the plain loop this replaces
        // stopped there and the two behind it were never closed.
        RecordingWriter failing = new RecordingWriter("bucket-0", true);
        RecordingWriter second = new RecordingWriter("bucket-1", false);
        RecordingWriter third = new RecordingWriter("bucket-2", false);
        putWriters(write, failing, second, third);

        assertThatThrownBy(write::close).hasMessage("close failed in bucket-0");

        assertThat(failing.closed).as("the failing writer").isTrue();
        assertThat(second.closed).as("the writer after the failure").isTrue();
        assertThat(third.closed).as("the last writer").isTrue();
    }

    @Test
    public void testLaterFailuresRideAlongInsteadOfBeingDropped() throws Exception {
        BucketedAppendFileStoreWrite write = newWrite();

        putWriters(
                write,
                new RecordingWriter("bucket-0", true),
                new RecordingWriter("bucket-1", true));

        assertThatThrownBy(write::close)
                .hasMessage("close failed in bucket-0")
                .satisfies(
                        thrown ->
                                assertThat(thrown.getSuppressed())
                                        .as("the second failure")
                                        .hasSize(1)
                                        .allSatisfy(
                                                s ->
                                                        assertThat(s)
                                                                .hasMessage(
                                                                        "close failed in bucket-1")));
    }

    @Test
    public void testWriterMapIsClearedEvenWhenAWriterThrows() throws Exception {
        BucketedAppendFileStoreWrite write = newWrite();
        putWriters(write, new RecordingWriter("bucket-0", true));

        assertThatThrownBy(write::close).hasMessage("close failed in bucket-0");

        // writers.clear() sits after the loop, together with the two executor shutdowns, so a
        // throwing writer used to skip all of them.
        assertThat(write.writers()).as("writers map after a failing close").isEmpty();
    }

    @Test
    public void testCloseSucceedsWhenNoWriterThrows() throws Exception {
        BucketedAppendFileStoreWrite write = newWrite();
        RecordingWriter a = new RecordingWriter("bucket-0", false);
        RecordingWriter b = new RecordingWriter("bucket-1", false);
        putWriters(write, a, b);

        write.close();

        assertThat(a.closed).isTrue();
        assertThat(b.closed).isTrue();
        assertThat(write.writers()).isEmpty();
    }

    private void putWriters(BucketedAppendFileStoreWrite write, RecordingWriter... writers) {
        HashMap<Integer, AbstractFileStoreWrite.WriterContainer<InternalRow>> bucketWriters =
                new HashMap<>();
        for (int i = 0; i < writers.length; i++) {
            bucketWriters.put(
                    i,
                    new AbstractFileStoreWrite.WriterContainer<>(
                            writers[i], 1, null, null, null, null));
        }
        write.writers().put(partition(0), bucketWriters);
    }

    private BucketedAppendFileStoreWrite newWrite() throws Exception {
        return (BucketedAppendFileStoreWrite) createFileStoreTable().store().newWrite("ss");
    }

    private static BinaryRow partition(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, i);
        writer.complete();
        return binaryRow;
    }

    private FileStoreTable createFileStoreTable() throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .column("f1", DataTypes.INT())
                        .column("f2", DataTypes.INT())
                        .partitionKeys("f0")
                        .option("bucket", "100")
                        .option("bucket-key", "f1")
                        .build();
        Identifier identifier = Identifier.create("default", "test");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    /**
     * Records whether it was closed, and optionally throws when it is. {@code close()} is the only
     * method {@link AbstractFileStoreWrite#close()} reaches.
     */
    private static class RecordingWriter implements RecordWriter<InternalRow> {

        private final String name;
        private final boolean throwOnClose;
        private boolean closed = false;

        private RecordingWriter(String name, boolean throwOnClose) {
            this.name = name;
            this.throwOnClose = throwOnClose;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            if (throwOnClose) {
                throw new Exception("close failed in " + name);
            }
        }

        @Override
        public void write(InternalRow record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void compact(boolean fullCompaction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addNewFiles(List<DataFileMeta> files) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<DataFileMeta> dataFiles() {
            return Collections.emptyList();
        }

        @Override
        public long maxSequenceNumber() {
            return 0;
        }

        @Override
        public CommitIncrement prepareCommit(boolean waitCompaction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean compactNotCompleted() {
            return false;
        }

        @Override
        public void sync() {}
    }
}
