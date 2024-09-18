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
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestCacheFilter;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.ChangelogDeletion;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitCallback;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.util.List;

/** {@link FileStore} with privilege checks. */
public class PrivilegedFileStore<T> implements FileStore<T> {

    private final FileStore<T> wrapped;
    private final PrivilegeChecker privilegeChecker;
    private final Identifier identifier;

    public PrivilegedFileStore(
            FileStore<T> wrapped, PrivilegeChecker privilegeChecker, Identifier identifier) {
        this.wrapped = wrapped;
        this.privilegeChecker = privilegeChecker;
        this.identifier = identifier;
    }

    @Override
    public FileStorePathFactory pathFactory() {
        return wrapped.pathFactory();
    }

    @Override
    public SnapshotManager snapshotManager() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.snapshotManager();
    }

    @Override
    public RowType partitionType() {
        return wrapped.partitionType();
    }

    @Override
    public CoreOptions options() {
        return wrapped.options();
    }

    @Override
    public BucketMode bucketMode() {
        return wrapped.bucketMode();
    }

    @Override
    public ManifestsReader newManifestsReader(boolean forWrite) {
        return wrapped.newManifestsReader(forWrite);
    }

    @Override
    public FileStoreScan newScan() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newScan();
    }

    @Override
    public ManifestList.Factory manifestListFactory() {
        return wrapped.manifestListFactory();
    }

    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return wrapped.manifestFileFactory();
    }

    @Override
    public IndexManifestFile.Factory indexManifestFileFactory() {
        return wrapped.indexManifestFileFactory();
    }

    @Override
    public IndexFileHandler newIndexFileHandler() {
        return wrapped.newIndexFileHandler();
    }

    @Override
    public StatsFileHandler newStatsFileHandler() {
        return wrapped.newStatsFileHandler();
    }

    @Override
    public SplitRead<T> newRead() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newRead();
    }

    @Override
    public FileStoreWrite<T> newWrite(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser);
    }

    @Override
    public FileStoreWrite<T> newWrite(String commitUser, ManifestCacheFilter manifestFilter) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newWrite(commitUser, manifestFilter);
    }

    @Override
    public FileStoreCommit newCommit(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newCommit(commitUser);
    }

    @Override
    public FileStoreCommit newCommit(String commitUser, List<CommitCallback> callbacks) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newCommit(commitUser, callbacks);
    }

    @Override
    public SnapshotDeletion newSnapshotDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newSnapshotDeletion();
    }

    @Override
    public ChangelogDeletion newChangelogDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newChangelogDeletion();
    }

    @Override
    public TagManager newTagManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagManager();
    }

    @Override
    public TagDeletion newTagDeletion() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagDeletion();
    }

    @Nullable
    @Override
    public PartitionExpire newPartitionExpire(String commitUser) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newPartitionExpire(commitUser);
    }

    @Override
    public TagAutoManager newTagCreationManager() {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.newTagCreationManager();
    }

    @Override
    public ServiceManager newServiceManager() {
        privilegeChecker.assertCanSelect(identifier);
        return wrapped.newServiceManager();
    }

    @Override
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        privilegeChecker.assertCanInsert(identifier);
        return wrapped.mergeSchema(rowType, allowExplicitCast);
    }

    @Override
    public List<TagCallback> createTagCallbacks() {
        return wrapped.createTagCallbacks();
    }

    @Override
    public void setManifestCache(SegmentsCache<Path> manifestCache) {
        wrapped.setManifestCache(manifestCache);
    }
}
