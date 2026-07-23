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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/** Tests for {@link TableDescriptorSerializer}. */
public class TableDescriptorSerializerTest {

    @TempDir java.nio.file.Path tempDir;

    private FileStoreTable createTable() {
        return createTable(Collections.singletonMap("bucket", "2"));
    }

    /**
     * Create a real table with schema-0 persisted on disk, so {@code schemaManager().schema(id)}
     * resolves (the serializer reads the persisted schema).
     */
    private FileStoreTable createTable(Map<String, String> options) {
        Path tablePath = new Path(tempDir.toString());
        LocalFileIO fileIO = LocalFileIO.create();
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "f0", new IntType()),
                                new DataField(1, "f1", new IntType())),
                        Collections.emptyList(),
                        Arrays.asList("f0", "f1"),
                        options,
                        "comment");
        try {
            new SchemaManager(fileIO, tablePath).createTable(schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return FileStoreTableFactory.create(fileIO, tablePath);
    }

    @Test
    public void testFromFileStoreTable() {
        FileStoreTable table = createTable();
        TableDescriptor descriptor = TableDescriptorSerializer.from(table, "db", "t");

        assertThat(descriptor.version()).isEqualTo(TableDescriptor.CURRENT_VERSION);
        assertThat(descriptor.path()).isEqualTo(table.location().toString());
        assertThat(descriptor.tableSchema().id()).isEqualTo(table.schema().id());
        assertThat(descriptor.database()).isEqualTo("db");
        assertThat(descriptor.name()).isEqualTo("t");
    }

    @Test
    public void testFromWithoutNames() {
        FileStoreTable table = createTable();
        TableDescriptor descriptor = TableDescriptorSerializer.from(table);
        assertThat(descriptor.database()).isNull();
        assertThat(descriptor.name()).isNull();
        assertThat(descriptor.path()).isEqualTo(table.location().toString());
    }

    @Test
    public void testSerializeRoundTripsThroughRustCompatibleJson() {
        FileStoreTable table = createTable();
        String json = TableDescriptorSerializer.serialize(table, "db", "t");

        TableDescriptor parsed = JsonSerdeUtil.fromJson(json, TableDescriptor.class);
        assertThat(parsed.version()).isEqualTo(TableDescriptor.CURRENT_VERSION);
        assertThat(parsed.path()).isEqualTo(table.location().toString());
        assertThat(parsed.tableSchema().id()).isEqualTo(table.schema().id());
        assertThat(parsed.tableSchema().primaryKeys()).containsExactly("f0", "f1");
        assertThat(parsed.tableSchema().options()).containsEntry("bucket", "2");
    }

    @Test
    public void testRejectsPartialIdentifier() {
        FileStoreTable table = createTable();
        assertThatThrownBy(() -> TableDescriptorSerializer.from(table, "db", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableDescriptorSerializer.from(table, null, "t"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testRejectsCompositeTable() {
        // Delegating/composite tables (fallback / chain / privileged) delegate location()/schema()
        // to their primary, so they would serialize as a plain table and silently drop their
        // merge/fallback semantics. v1 must reject them.
        assertThatThrownBy(
                        () ->
                                TableDescriptorSerializer.from(
                                        mock(FallbackReadFileStoreTable.class)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testDoesNotLeakDynamicCredentials() {
        // table.schema() would carry dynamic session options (incl. credentials); the serializer
        // must ship the persisted schema instead.
        FileStoreTable table =
                createTable().copy(Collections.singletonMap("fs.s3a.secret.key", "review-secret"));

        String json = TableDescriptorSerializer.serialize(table, "db", "t");
        assertThat(json).doesNotContain("review-secret");

        TableDescriptor descriptor = TableDescriptorSerializer.from(table, "db", "t");
        assertThat(descriptor.tableSchema().options()).doesNotContainKey("fs.s3a.secret.key");
        // The persisted schema also drops the runtime-injected path option.
        assertThat(descriptor.tableSchema().options()).doesNotContainKey("path");
    }

    @Test
    public void testCarriesNonMainBranch() {
        // A non-main branch (of a plain table) is carried so the reader reads the right branch.
        // branch lives only in the resolved table's options, never in the persisted schema, so it
        // is derived from the table; the persisted schema is read (branch-scoped) from the branch.
        Path tablePath = new Path(tempDir.toString());
        LocalFileIO fileIO = LocalFileIO.create();
        Schema schema =
                new Schema(
                        Arrays.asList(
                                new DataField(0, "f0", new IntType()),
                                new DataField(1, "f1", new IntType())),
                        Collections.emptyList(),
                        Arrays.asList("f0", "f1"),
                        Collections.singletonMap("bucket", "2"),
                        "comment");
        try {
            new SchemaManager(fileIO, tablePath).createTable(schema);
            new SchemaManager(fileIO, tablePath, "b1").createTable(schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        FileStoreTable table =
                FileStoreTableFactory.create(fileIO, tablePath)
                        .copy(Collections.singletonMap("branch", "b1"));

        TableDescriptor descriptor = TableDescriptorSerializer.from(table, "db", "t");
        assertThat(descriptor.branch()).isEqualTo("b1");
        // path stays the table root (branch-independent); branch travels in its own field.
        assertThat(descriptor.path()).isEqualTo(table.location().toString());
        assertThat(descriptor.tableSchema().options()).doesNotContainKey("branch");
    }

    @Test
    public void testCarriesResolvedSnapshotIdForTimeTravel() {
        // A time-travel request (scan.snapshot-id/tag/timestamp/...) is resolved to a concrete
        // snapshot at serialize time and shipped as snapshotId, so a reader planning from the
        // descriptor reproduces exactly that snapshot instead of reading latest.
        FileStoreTable table = createTable();
        writeOneRow(table); // creates snapshot 1

        FileStoreTable travel = table.copy(Collections.singletonMap("scan.snapshot-id", "1"));
        TableDescriptor descriptor = TableDescriptorSerializer.from(travel, "db", "t");
        assertThat(descriptor.snapshotId()).isEqualTo(1L);
        // The persisted schema must not carry the scan option.
        assertThat(descriptor.tableSchema().options()).doesNotContainKey("scan.snapshot-id");
    }

    @Test
    public void testNoSnapshotIdForOrdinaryScan() {
        FileStoreTable table = createTable();
        writeOneRow(table);
        TableDescriptor descriptor = TableDescriptorSerializer.from(table, "db", "t");
        assertThat(descriptor.snapshotId()).isNull();
    }

    private static void writeOneRow(FileStoreTable table) {
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
                BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            write.write(GenericRow.of(1, 1));
            commit.commit(write.prepareCommit());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
