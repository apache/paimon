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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.FileSystemBranchManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/** Tests filesystem failures when renaming persisted table tags and branches. */
class MetadataRenameTest extends TableTestBase {

    @Test
    void testRejectedTagRenameCanBeRetried() throws Exception {
        FileStoreTable original = createTaggedTable();
        FileIO fileIO = spy(original.fileIO());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, original.location());
        Path source = table.tagManager().tagPath("old");
        Path target = table.tagManager().tagPath("new");
        doReturn(false).when(fileIO).rename(source, target);

        assertThatThrownBy(() -> table.renameTag("old", "new"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to rename tag 'old' to 'new'.");
        assertThat(table.tagManager().tagExists("old")).isTrue();
        assertThat(table.tagManager().tagExists("new")).isFalse();

        doCallRealMethod().when(fileIO).rename(source, target);
        table.renameTag("old", "new");
        assertThat(table.tagManager().tagExists("old")).isFalse();
        assertThat(table.tagManager().getOrThrow("new").id())
                .isEqualTo(original.latestSnapshot().get().id());
    }

    @Test
    void testTagRenamePreservesIOException() throws Exception {
        FileStoreTable original = createTaggedTable();
        FileIO fileIO = spy(original.fileIO());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, original.location());
        IOException failure = new IOException("rename unavailable");
        doThrow(failure)
                .when(fileIO)
                .rename(table.tagManager().tagPath("old"), table.tagManager().tagPath("new"));

        assertThatThrownBy(() -> table.renameTag("old", "new"))
                .isInstanceOf(RuntimeException.class)
                .hasCause(failure);
        assertThat(table.tagManager().tagExists("old")).isTrue();
        assertThat(table.tagManager().tagExists("new")).isFalse();
    }

    @Test
    void testRejectedBranchRenameCanBeRetried() throws Exception {
        FileStoreTable original = createTaggedTable();
        original.createBranch("old", "old");
        FileIO fileIO = spy(original.fileIO());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, original.location());
        FileSystemBranchManager branches = (FileSystemBranchManager) table.branchManager();
        Path source = branches.branchPath("old");
        Path target = branches.branchPath("new");
        doReturn(false).when(fileIO).rename(source, target);

        assertThatThrownBy(() -> table.renameBranch("old", "new"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to rename branch 'old' to 'new'.");
        assertThat(branches.branchExists("old")).isTrue();
        assertThat(branches.branchExists("new")).isFalse();

        doCallRealMethod().when(fileIO).rename(source, target);
        table.renameBranch("old", "new");
        assertThat(branches.branchExists("old")).isFalse();
        assertThat(table.switchToBranch("new").latestSnapshot().get().id())
                .isEqualTo(original.latestSnapshot().get().id());
    }

    @Test
    void testBranchRenamePreservesIOException() throws Exception {
        FileStoreTable original = createTaggedTable();
        original.createBranch("old", "old");
        FileIO fileIO = spy(original.fileIO());
        FileStoreTable table = FileStoreTableFactory.create(fileIO, original.location());
        FileSystemBranchManager branches = (FileSystemBranchManager) table.branchManager();
        IOException failure = new IOException("rename unavailable");
        doThrow(failure)
                .when(fileIO)
                .rename(branches.branchPath("old"), branches.branchPath("new"));

        assertThatThrownBy(() -> table.renameBranch("old", "new"))
                .isInstanceOf(RuntimeException.class)
                .hasCause(failure);
        assertThat(branches.branchExists("old")).isTrue();
        assertThat(branches.branchExists("new")).isFalse();
    }

    private FileStoreTable createTaggedTable() throws Exception {
        catalog.createTable(
                identifier(), Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        write(table, GenericRow.of(1));
        table.createTag("old");
        return table;
    }
}
