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

package org.apache.paimon.io;

import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;

import javax.annotation.Nullable;

import java.util.Map;

/** Factory which produces read {@link Path}s data files for chain tbl. */
public class ChainReadDataFilePathFactory extends DataFilePathFactory {

    private final Map<String, String> fileBucketPathMapping;

    public ChainReadDataFilePathFactory(
            Path parent,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable ExternalPathProvider externalPathProvider,
            Map<String, String> fileBucketPathMapping) {
        super(
                parent,
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                externalPathProvider);
        this.fileBucketPathMapping = fileBucketPathMapping;
    }

    @Override
    public Path newPath(String prefix) {
        throw new UnsupportedOperationException("Please use api with relativeBucketPath");
    }

    @Override
    public Path toPath(DataFileMeta file) {
        return file.externalPath()
                .map(Path::new)
                .orElse(new Path(fileBucketPathMapping.get(file.fileName()), file.fileName()));
    }

    @Override
    public Path toPath(FileEntry file) {
        throw new UnsupportedOperationException("Please use api with relativeBucketPath");
    }

    @Override
    public Path toAlignedPath(String fileName, DataFileMeta aligned) {
        throw new UnsupportedOperationException("Please use api with relativeBucketPath");
    }

    @Override
    public Path toAlignedPath(String fileName, FileEntry aligned) {
        throw new UnsupportedOperationException("Please use api with relativeBucketPath");
    }
}
