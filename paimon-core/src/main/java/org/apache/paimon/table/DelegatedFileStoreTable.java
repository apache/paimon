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
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** Delegated {@link FileStoreTable}. */
public abstract class DelegatedFileStoreTable implements FileStoreTable {

    protected final FileStoreTable wrapped;

    public DelegatedFileStoreTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    public FileStoreTable wrapped() {
        return wrapped;
    }

    @Override
    public String name() {
        return wrapped.name();
    }

    @Override
    public String fullName() {
        return wrapped.fullName();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
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
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
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
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        wrapped.setManifestCache(manifestCache);
    }

    @Override
    public TableSchema schema() {
        return wrapped.schema();
    }

    @Override
    public FileStore<?> store() {
        return wrapped.store();
    }

    @Override
    public CatalogEnvironment catalogEnvironment() {
        return wrapped.catalogEnvironment();
    }

    @Override
    public Optional<Statistics> statistics() {
        return wrapped.statistics();
    }

    @Override
    public OptionalLong latestSnapshotId() {
        return wrapped.latestSnapshotId();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public void rollbackTo(long snapshotId) {
        wrapped.rollbackTo(snapshotId);
    }

    @Override
    public void createTag(String tagName) {
        wrapped.createTag(tagName);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId) {
        wrapped.createTag(tagName, fromSnapshotId);
    }

    @Override
    public void createTag(String tagName, Duration timeRetained) {
        wrapped.createTag(tagName, timeRetained);
    }

    @Override
    public void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        wrapped.createTag(tagName, fromSnapshotId, timeRetained);
    }

    @Override
    public void renameTag(String tagName, String newTagName) {
        wrapped.renameTag(tagName, newTagName);
    }

    @Override
    public void deleteTag(String tagName) {
        wrapped.deleteTag(tagName);
    }

    @Override
    public void rollbackTo(String tagName) {
        wrapped.rollbackTo(tagName);
    }

    @Override
    public void createBranch(String branchName) {
        wrapped.createBranch(branchName);
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        wrapped.createBranch(branchName, tagName);
    }

    @Override
    public void deleteBranch(String branchName) {
        wrapped.deleteBranch(branchName);
    }

    @Override
    public void fastForward(String branchName) {
        wrapped.fastForward(branchName);
    }

    @Override
    public ExpireSnapshots newExpireSnapshots() {
        return wrapped.newExpireSnapshots();
    }

    @Override
    public ExpireSnapshots newExpireChangelog() {
        return wrapped.newExpireChangelog();
    }

    @Override
    public DataTableScan newScan() {
        return wrapped.newScan();
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        return wrapped.newStreamScan();
    }

    @Override
    public InnerTableRead newRead() {
        return wrapped.newRead();
    }

    @Override
    public Optional<WriteSelector> newWriteSelector() {
        return wrapped.newWriteSelector();
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser) {
        return wrapped.newWrite(commitUser);
    }

    @Override
    public TableWriteImpl<?> newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        return wrapped.newWrite(commitUser, manifestFilter);
    }

    @Override
    public TableCommitImpl newCommit(String commitUser) {
        return wrapped.newCommit(commitUser);
    }

    @Override
    public LocalTableQuery newLocalTableQuery() {
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
        DelegatedFileStoreTable that = (DelegatedFileStoreTable) o;
        return Objects.equals(wrapped, that.wrapped);
    }
}
