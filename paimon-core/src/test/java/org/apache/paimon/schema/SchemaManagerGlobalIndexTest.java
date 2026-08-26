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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests schema changes which affect columns referenced by live Global Index files. */
public class SchemaManagerGlobalIndexTest {

    private static final int INDEX_FIELD_ID = 1;
    private static final int EXTRA_FIELD_ID = 2;

    @TempDir java.nio.file.Path tempDir;

    private SchemaManager schemaManager;
    private FileStoreTable table;
    private IndexFileMeta globalIndex;

    @BeforeEach
    public void beforeEach() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toString());
        schemaManager = new SchemaManager(fileIO, tablePath);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "-1");
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaManager.createTable(
                new Schema(
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(INDEX_FIELD_ID, "indexed_col", DataTypes.INT()),
                                new DataField(EXTRA_FIELD_ID, "extra_col", DataTypes.INT()),
                                new DataField(3, "other_col", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        options,
                        null));

        table = FileStoreTableFactory.create(fileIO, tablePath);
        writeOneRow();
        globalIndex =
                new IndexFileMeta(
                        "test-global-index",
                        "global-index-file",
                        1,
                        1,
                        new GlobalIndexMeta(0, 0, INDEX_FIELD_ID, new int[] {EXTRA_FIELD_ID}, null),
                        null);
        commitIndex(DataIncrement.indexIncrement(Collections.singletonList(globalIndex)));
    }

    @Test
    public void testRejectReferencedGlobalIndexColumnChanges() {
        assertReferencedChangeRejected(SchemaChange.dropColumn("indexed_col"));
        assertReferencedChangeRejected(
                SchemaChange.renameColumn("indexed_col", "renamed_indexed_col"));
        assertReferencedChangeRejected(
                SchemaChange.updateColumnType("indexed_col", DataTypes.BIGINT()));

        assertReferencedChangeRejected(SchemaChange.dropColumn("extra_col"));
        assertReferencedChangeRejected(SchemaChange.renameColumn("extra_col", "renamed_extra_col"));
        assertReferencedChangeRejected(
                SchemaChange.updateColumnType("extra_col", DataTypes.BIGINT()));
    }

    @Test
    public void testAllowUnrelatedSchemaChanges() {
        assertThatCode(
                        () ->
                                schemaManager.commitChanges(
                                        SchemaChange.renameColumn("other_col", "renamed_other"),
                                        SchemaChange.updateColumnComment(
                                                new String[] {"indexed_col"}, "comment")))
                .doesNotThrowAnyException();

        assertThat(schemaManager.latest().get().fieldNames())
                .containsExactly("id", "indexed_col", "extra_col", "renamed_other");
    }

    @Test
    public void testAllowSchemaChangeAfterDroppingGlobalIndex() throws Exception {
        commitIndex(DataIncrement.deleteIndexIncrement(Collections.singletonList(globalIndex)));

        assertThatCode(
                        () ->
                                schemaManager.commitChanges(
                                        SchemaChange.dropColumn("indexed_col"),
                                        SchemaChange.renameColumn("extra_col", "renamed_extra")))
                .doesNotThrowAnyException();

        assertThat(schemaManager.latest().get().fieldNames())
                .containsExactly("id", "renamed_extra", "other_col");
    }

    private void assertReferencedChangeRejected(SchemaChange change) {
        assertThatThrownBy(() -> schemaManager.commitChanges(change))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("referenced by live Global Index files")
                .hasMessageContaining("indexed-field-ids=[1, 2]")
                .hasMessageContaining("Drop the complete Global Index");
    }

    private void writeOneRow() throws Exception {
        String commitUser = "write-row";
        try (TableWriteImpl<?> write = table.newWrite(commitUser);
                TableCommitImpl commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(0, 1, 2, 3));
            commit.commit(write.prepareCommit(false, 1));
        }
    }

    private void commitIndex(DataIncrement increment) throws Exception {
        try (TableCommitImpl commit = table.newCommit("global-index")) {
            commit.commit(
                    Collections.singletonList(
                            new CommitMessageImpl(
                                    BinaryRow.EMPTY_ROW,
                                    UNAWARE_BUCKET,
                                    null,
                                    increment,
                                    CompactIncrement.emptyIncrement())));
        }
    }
}
