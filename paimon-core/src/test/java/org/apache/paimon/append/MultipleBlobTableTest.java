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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.DataEvolutionSplitRead.FieldBunch;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.operation.DataEvolutionSplitRead.splitFieldBunches;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with blob. */
public class MultipleBlobTableTest extends TableTestBase {

    private final byte[] blobBytes1 = randomBytes();
    private final byte[] blobBytes2 = randomBytes();

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        AtomicInteger integer = new AtomicInteger(0);

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> filesMetas =
                table.store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .collect(Collectors.toList());

        RowType rowType = table.schema().logicalRowType();
        List<FieldBunch> fieldGroups = splitFieldBunches(filesMetas, file -> rowType);

        assertThat(fieldGroups.size()).isEqualTo(3);
        assertThat(fieldGroups.get(0).files().size()).isEqualTo(1);
        assertThat(fieldGroups.get(1).files().size()).isEqualTo(10);
        assertThat(fieldGroups.get(2).files().size()).isEqualTo(10);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes1);
                        assertThat(row.getBlob(3).toData()).isEqualTo(blobBytes2);
                    }
                });

        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testDataEvolutionBlobCompaction() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(50, 20));

        FileStoreTable table = getTableDefault();
        List<DataFileMeta> before = currentDataFiles(table);
        long beforeBlobFileCount =
                before.stream().filter(file -> isBlobFile(file.fileName())).count();
        assertThat(beforeBlobFileCount).isEqualTo(40);

        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, true, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(DataEvolutionCompactTask::isBlobTask)).isTrue();

        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : tasks) {
            compactMessages.add(task.doCompact(table, commitUser));
        }
        commitDefault(compactMessages);

        List<DataFileMeta> after = currentDataFiles(table);
        long afterBlobFileCount =
                after.stream().filter(file -> isBlobFile(file.fileName())).count();
        assertThat(afterBlobFileCount).isLessThan(beforeBlobFileCount);
        coordinator = new DataEvolutionCompactCoordinator(table, true, false);
        assertThat(coordinator.plan().stream().anyMatch(DataEvolutionCompactTask::isBlobTask))
                .isFalse();

        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        assertThat(row.getBlob(2).toData()).isEqualTo(blobBytes1);
                        assertThat(row.getBlob(3).toData()).isEqualTo(blobBytes2);
                    }
                });
        assertThat(integer.get()).isEqualTo(1000);
    }

    @Test
    public void testDataEvolutionBlobCompactionAfterDropBlobColumns() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(1000, 1));

        catalog.alterTable(
                identifier(),
                Arrays.asList(SchemaChange.dropColumn("f2"), SchemaChange.dropColumn("f3")),
                false);

        FileStoreTable table = getTableDefault();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, true, false);
        List<DataEvolutionCompactTask> tasks = coordinator.plan();
        assertThat(tasks.stream().anyMatch(DataEvolutionCompactTask::isBlobTask)).isFalse();

        List<DataFileMeta> after = currentDataFiles(getTableDefault());
        assertThat(after.stream().filter(file -> isBlobFile(file.fileName())).count())
                .isEqualTo(20);

        AtomicInteger integer = new AtomicInteger(0);
        readDefault(
                row -> {
                    integer.incrementAndGet();
                    assertThat(row.getFieldCount()).isEqualTo(2);
                });
        assertThat(integer.get()).isEqualTo(1000);
    }

    private List<DataFileMeta> currentDataFiles(FileStoreTable table) {
        return table.store().newScan().plan().files().stream()
                .map(ManifestEntry::file)
                .collect(Collectors.toList());
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.BLOB());
        schemaBuilder.column("f3", DataTypes.BLOB());
        schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "1 GB");
        schemaBuilder.option(CoreOptions.BLOB_TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        return schemaBuilder.build();
    }

    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(),
                BinaryString.fromBytes(randomBytes()),
                new BlobData(blobBytes1),
                new BlobData(blobBytes2));
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[2 * 1024 * 124];
        RANDOM.nextBytes(binary);
        return binary;
    }
}
