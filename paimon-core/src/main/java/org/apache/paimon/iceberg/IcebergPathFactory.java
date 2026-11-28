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

package org.apache.paimon.iceberg;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.PathFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Stream;

/** Path factory for Iceberg metadata files. */
public class IcebergPathFactory {

    private final Path metadataDirectory;
    private final String uuid;

    private int manifestFileCount;
    private int manifestListCount;

    @Nullable private List<Path> recorded;

    public IcebergPathFactory(Path metadataDirectory) {
        this.metadataDirectory = metadataDirectory;
        this.uuid = UUID.randomUUID().toString();
    }

    public Path metadataDirectory() {
        return metadataDirectory;
    }

    /** Starts remembering every manifest and manifest list path handed out from now on. */
    public void startRecording() {
        recorded = new ArrayList<>();
    }

    /** The paths handed out since recording started. */
    public List<Path> recordedPaths() {
        return recorded == null ? new ArrayList<>() : new ArrayList<>(recorded);
    }

    /** Stops remembering new paths. */
    public void stopRecording() {
        recorded = null;
    }

    private Path record(Path path) {
        if (recorded != null) {
            recorded.add(path);
        }
        return path;
    }

    public Path newManifestFile() {
        manifestFileCount++;
        return record(toManifestFilePath(uuid + "-m" + manifestFileCount + ".avro"));
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(metadataDirectory(), manifestFileName);
    }

    public Path newManifestListFile() {
        manifestListCount++;
        return record(toManifestListPath("snap-" + manifestListCount + "-" + uuid + ".avro"));
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(metadataDirectory(), manifestListName);
    }

    public Path toMetadataPath(long snapshotId) {
        return new Path(
                metadataDirectory(), String.format(Locale.ROOT, "v%d.metadata.json", snapshotId));
    }

    public Path toMetadataPath(String metadataName) {
        return new Path(metadataDirectory(), metadataName);
    }

    public Stream<Path> getAllMetadataPathBefore(FileIO fileIO, long snapshotId)
            throws IOException {
        return FileUtils.listVersionedFileStatus(fileIO, metadataDirectory, "v")
                .map(FileStatus::getPath)
                .filter(
                        path -> {
                            try {
                                String id = path.getName().split("\\.")[0].substring(1);
                                return Long.parseLong(id) < snapshotId;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        });
    }

    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName);
            }
        };
    }

    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestListFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName);
            }
        };
    }
}
