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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** {@link FileStoreTable} with privilege checks. */
public class PrivilegedFileStoreTable implements FileStoreTable {

    private final FileStoreTable wrapped;
    private final PrivilegeChecker privilegeChecker;
    private final Identifier identifier;

    public PrivilegedFileStoreTable(
            FileStoreTable wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        this.wrapped = wrapped;
        this.privilegeChecker = privilegeChecker;
        this.identifier = identifier;
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newSnapshotReader();
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
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
    public Path location() {
        return wrapped.location();
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }

    @Override
    public TableSchema schema() {
        return wrapped.schema();
    }

    @Override
    public FileStore<?> store() {
        return new PrivilegedFileStore<>(wrapped.store(), privilegeChecker, identifier);
    }

    @Override
    public BucketMode bucketMode() {
        return wrapped.bucketMode();
    }

    @Override
    public CatalogEnvironment catalogEnvironment() {
        return wrapped.catalogEnvironment();
    }

    @Override
    public Optional<Statistics> statistics() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.statistics();
    }

    @Override
    public FileStoreTable copy(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public FileStoreTable copy(TableSchema newTableSchema) {
        return new PrivilegedFileStoreTable(
                wrapped.copy(newTableSchema), privilegeChecker, identifier);
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
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createTag(tagName, fromSnapshotId, timeRetained);
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
    public void createBranch(String branchName, long snapshotId) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.createBranch(branchName, snapshotId);
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
    public void replaceBranch(String fromBranch) {
        privilegeChecker.assertCanInsert(identifier);
        wrapped.replaceBranch(fromBranch);
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
    public FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions) {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithoutTimeTravel(dynamicOptions), privilegeChecker, identifier);
    }

    @Override
    public FileStoreTable copyWithLatestSchema() {
        return new PrivilegedFileStoreTable(
                wrapped.copyWithLatestSchema(), privilegeChecker, identifier);
    }

    @Override
    public InnerTableScan newScan() {
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
    public TableWriteImpl<?> newWrite(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, manifestFilter);
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

    @Override
    public boolean supportStreamingReadOverwrite() {
        return wrapped.supportStreamingReadOverwrite();
    }

    @Override
    public RowKeyExtractor createRowKeyExtractor() {
        return wrapped.createRowKeyExtractor();
    }

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
}
