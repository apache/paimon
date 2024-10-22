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

import org.apache.paimon.KeyValue;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.AbstractFileStoreWrite.WriterContainer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;

/** Tests for {@link AbstractFileStoreWrite}. */
public class AbstractFileStoreWriteTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCloseWriterContainer() throws Exception {
        FileStoreTable table = createFileStoreTable();
        KeyValueFileStoreWrite write =
                (KeyValueFileStoreWrite)
                        table.store()
                                .newWrite("ss")
                                .withIOManager(new IOManagerImpl(tempDir.toString()));
        write.withExecutionMode(false);

        write.write(
                partition(0),
                0,
                new KeyValue().replace(getBinaryRow(0), RowKind.INSERT, GenericRow.of(0, 0)));

        Assertions.assertThat(write.writers.size()).isEqualTo(1);
        Assertions.assertThat(write.writers.get(partition(0)).size()).isEqualTo(1);

        AbstractFileStoreWrite.WriterContainer<KeyValue> writerContainer =
                write.writers.get(partition(0)).get(0);

        Assertions.assertThat(writerContainer.indexMaintainer).isNotNull();
        Assertions.assertThat(writerContainer.deletionVectorsMaintainer).isNotNull();

        Set<Integer> bucketsToRemove = new ConcurrentSkipListSet<>();
        Function<WriterContainer<KeyValue>, Boolean> writerCleanChecker =
                tmpWriterContainer -> false;
        CommitMessageImpl commitMessage =
                (CommitMessageImpl)
                        write.closeWriterContainer(
                                partition(0),
                                0,
                                writerContainer,
                                false,
                                writerCleanChecker,
                                100,
                                bucketsToRemove);

        long records =
                commitMessage.newFilesIncrement().newFiles().stream()
                        .mapToLong(DataFileMeta::rowCount)
                        .sum();
        Assertions.assertThat(records).isEqualTo(1);

        // add new records
        write.write(
                partition(0),
                0,
                new KeyValue().replace(getBinaryRow(1), RowKind.INSERT, GenericRow.of(1, 1)));
        write.write(
                partition(0),
                0,
                new KeyValue().replace(getBinaryRow(2), RowKind.INSERT, GenericRow.of(2, 2)));
        commitMessage =
                (CommitMessageImpl)
                        write.closeWriterContainer(
                                partition(0),
                                0,
                                writerContainer,
                                false,
                                writerCleanChecker,
                                200,
                                bucketsToRemove);
        records =
                commitMessage.newFilesIncrement().newFiles().stream()
                        .mapToLong(DataFileMeta::rowCount)
                        .sum();
        Assertions.assertThat(records).isEqualTo(2);

        // add one deletion
        write.write(
                partition(0),
                0,
                new KeyValue().replace(getBinaryRow(3), RowKind.INSERT, GenericRow.of(3, 3)));
        write.write(
                partition(0),
                0,
                new KeyValue().replace(getBinaryRow(4), RowKind.INSERT, GenericRow.of(4, 4)));
        writerContainer.deletionVectorsMaintainer.notifyNewDeletion("file_0", 0);

        commitMessage =
                (CommitMessageImpl)
                        write.closeWriterContainer(
                                partition(0),
                                0,
                                writerContainer,
                                false,
                                writerCleanChecker,
                                300,
                                bucketsToRemove);
        records =
                commitMessage.newFilesIncrement().newFiles().stream()
                        .mapToLong(DataFileMeta::rowCount)
                        .sum();
        Assertions.assertThat(records).isEqualTo(2);
    }

    private BinaryRow getBinaryRow(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, i);
        writer.complete();
        return binaryRow;
    }

    private BinaryRow partition(int i) {
        BinaryRow binaryRow = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(binaryRow);
        writer.writeInt(0, i);
        writer.complete();
        return binaryRow;
    }

    protected FileStoreTable createFileStoreTable() throws Exception {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "-1")
                        .option("deletion-vectors.enabled", "true")
                        .build();
        Identifier identifier = Identifier.create("default", "test");
        catalog.createDatabase("default", false);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }
}
