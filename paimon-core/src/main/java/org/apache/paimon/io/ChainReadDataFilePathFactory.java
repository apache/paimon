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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RoundRobinExternalPathProvider;
import org.apache.paimon.manifest.FileEntry;

import javax.annotation.Nullable;

import java.util.Optional;

/** Factory which produces read path for chain split. */
public class ChainReadDataFilePathFactory extends DataFilePathFactory {

    private final ChainReadContext chainReadContext;

    public ChainReadDataFilePathFactory(
            Path parent,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable RoundRobinExternalPathProvider externalPathProvider,
            ChainReadContext chainReadContext) {
        super(
                parent,
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                externalPathProvider);
        this.chainReadContext = chainReadContext;
    }

    @Override
    public Path toPath(DataFileMeta file) {
        return file.externalPath()
                .map(Path::new)
                .orElse(
                        new Path(
                                chainReadContext.fileBucketPathMapping().get(file.fileName()),
                                file.fileName()));
    }

    @Override
    public Path toPath(FileEntry file) {
        return Optional.ofNullable(file.externalPath())
                .map(Path::new)
                .orElse(
                        new Path(
                                chainReadContext.fileBucketPathMapping().get(file.fileName()),
                                file.fileName()));
    }
}
