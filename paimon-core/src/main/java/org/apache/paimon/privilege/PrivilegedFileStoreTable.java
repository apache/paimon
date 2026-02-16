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

package org.apache.paimon.privilege;

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.DelegatedFileStoreTable;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** {@link FileStoreTable} with privilege checks. */
public class PrivilegedFileStoreTable extends DelegatedFileStoreTable {

    protected final PrivilegeChecker privilegeChecker;
    protected final Identifier identifier;

    protected PrivilegedFileStoreTable(
            FileStoreTable wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        super(wrapped);
        this.privilegeChecker = privilegeChecker;
        this.identifier = identifier;
    }

    @Override
    public SnapshotManager snapshotManager() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.changelogManager();
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        privilegeChecker.assertCanSelectOrInsert(identifier);
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newSnapshotReader();
    }

    @Override
    public TagManager tagManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        privilegeChecker.assertCanSelect(identifier);
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.branchManager();
    }

    @Override
    public FileStore<?> store() {
        return new PrivilegedFileStore<>(wrapped.store(), privilegeChecker, identifier);
    }

    @Override
    public Optional<Statistics> statistics() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.statistics();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.rollbackTo(snapshotId);
    }

    @Override
    public void createTag(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, fromSnapshotId);
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, timeRetained);
    }

    @Override
    public void renameTag(String tagName, String targetTagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.renameTag(tagName, targetTagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public TagAutoManager newTagAutoManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagAutoManager();
    }

    @Override
    public void deleteTag(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.deleteTag(tagName);
    }

    @Override
    public void rollbackTo(String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.rollbackTo(tagName);
    }

    @Override
    public void createBranch(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.deleteBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.fastForward(branchName);
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newExpireSnapshots();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newExpireChangelog();
    }

    @Override
    public DataTableScan newScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newStreamScan();
    }

    @Override
    public InnerTableRead newRead() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newRead();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWriteSelector();
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, writeId);
    }

    @Override
    public TableWriteImpl<?> newWrite(
            String commitUser, @Nullable Integer writeId, RowKeyExtractor rowKeyExtractor) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, writeId, rowKeyExtractor);
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newCommit(commitUser);
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newLocalTableQuery();
    }

    // ======================= equals ============================

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegedFileStoreTable that = (PrivilegedFileStoreTable) o;
        return Objects.equals(wrapped, that.wrapped)
                && Objects.equals(privilegeChecker, that.privilegeChecker)
                && Objects.equals(identifier, that.identifier);
    }

    // ======================= copy ============================

    @Override
    public PrivilegedFileStoreTable copy(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copy(TableSchema newTableSchema) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(newTableSchema), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable copyWithLatestSchema() {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithLatestSchema(), privilegeChecker, identifier);
    }

    @Override
    public PrivilegedFileStoreTable switchToBranch(String branchName) {
        return new PrivilegedFileStoreTable(
                wrapped.switchToBranch(branchName), privilegeChecker, identifier);
    }

    public static PrivilegedFileStoreTable wrap(
            FileStoreTable table, PrivilegeChecker privilegeChecker, Identifier identifier) {
        return new PrivilegedFileStoreTable(table, privilegeChecker, identifier);
    }
}
