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

import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.PathFactory;

import java.util.UUID;

/** Path factory for Iceberg metadata files. */
public class IcebergPathFactory {

    private final Path root;
    private final String uuid;

    private int manifestFileCount;
    private int manifestListCount;

    public IcebergPathFactory(Path root) {
        this.root = root;
        this.uuid = UUID.randomUUID().toString();
    }

    public Path metadataDirectory() {
        return new Path(root, "metadata");
    }

    public Path newManifestFile() {
        manifestFileCount++;
        return toManifestFilePath(uuid + "-m" + manifestFileCount + ".avro");
    }

    public Path toManifestFilePath(String manifestFileName) {
        return new Path(metadataDirectory(), manifestFileName);
    }

    public Path newManifestListFile() {
        manifestListCount++;
        return toManifestListPath("snap-" + manifestListCount + "-" + uuid + ".avro");
    }

    public Path toManifestListPath(String manifestListName) {
        return new Path(metadataDirectory(), manifestListName);
    }

    public Path toMetadataPath(long snapshotId) {
        return new Path(metadataDirectory(), String.format("v%d.metadata.json", snapshotId));
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
