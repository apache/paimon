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

package org.apache.flink.table.store.file.operation;

import org.apache.flink.table.store.file.Snapshot;
import org.apache.flink.table.store.file.manifest.ManifestEntry;
import org.apache.flink.table.store.file.manifest.ManifestFile;
import org.apache.flink.table.store.file.manifest.ManifestFileMeta;
import org.apache.flink.table.store.file.manifest.ManifestList;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Default implementation of {@link FileStoreScan}. */
public class FileStoreScanImpl implements FileStoreScan {

    private final FileStorePathFactory pathFactory;
    private final ManifestFile manifestFile;
    private final ManifestList manifestList;

    private Long snapshotId;
    private List<ManifestFileMeta> manifests;

    public FileStoreScanImpl(
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList) {
        this.pathFactory = pathFactory;
        this.manifestFile = manifestFile;
        this.manifestList = manifestList;

        this.snapshotId = null;
        this.manifests = new ArrayList<>();
    }

    @Override
    public FileStoreScan withPartitionFilter(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStoreScan withKeyFilter(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStoreScan withValueFilter(Predicate predicate) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStoreScan withBucket(int bucket) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileStoreScan withSnapshot(long snapshotId) {
        this.snapshotId = snapshotId;
        Snapshot snapshot = Snapshot.fromPath(pathFactory.toSnapshotPath(snapshotId));
        this.manifests = manifestList.read(snapshot.manifestList());
        return this;
    }

    @Override
    public FileStoreScan withManifestList(List<ManifestFileMeta> manifests) {
        this.manifests = manifests;
        return this;
    }

    @Override
    public Plan plan() {
        List<ManifestEntry> files = scan();

        return new Plan() {
            @Nullable
            @Override
            public Long snapshotId() {
                return snapshotId;
            }

            @Override
            public List<ManifestEntry> files() {
                return files;
            }
        };
    }

    private List<ManifestEntry> scan() {
        Map<ManifestEntry.Identifier, ManifestEntry> map = new LinkedHashMap<>();
        for (ManifestFileMeta manifest : manifests) {
            // TODO read each manifest file concurrently
            for (ManifestEntry entry : manifestFile.read(manifest.fileName())) {
                ManifestEntry.Identifier identifier = entry.identifier();
                switch (entry.kind()) {
                    case ADD:
                        Preconditions.checkState(
                                !map.containsKey(identifier),
                                "Trying to add file %s which is already added. "
                                        + "Manifest might be corrupted.",
                                identifier);
                        map.put(identifier, entry);
                        break;
                    case DELETE:
                        Preconditions.checkState(
                                map.containsKey(identifier),
                                "Trying to delete file %s which is not previously added. "
                                        + "Manifest might be corrupted.",
                                identifier);
                        map.remove(identifier);
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown value kind " + entry.kind().name());
                }
            }
        }
        return new ArrayList<>(map.values());
    }
}
