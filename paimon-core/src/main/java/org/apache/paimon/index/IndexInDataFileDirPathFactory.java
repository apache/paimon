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

package org.apache.paimon.index;

import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFilePathFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.FileStorePathFactory.INDEX_PREFIX;

/** Path factory to create an index path. */
public class IndexInDataFileDirPathFactory implements IndexPathFactory {

    private final String uuid;
    private final DataFilePathFactory dataFilePathFactory;
    private final AtomicInteger indexFileCount;

    public IndexInDataFileDirPathFactory(
            String uuid, AtomicInteger indexFileCount, DataFilePathFactory dataFilePathFactory) {
        this.uuid = uuid;
        this.dataFilePathFactory = dataFilePathFactory;
        this.indexFileCount = indexFileCount;
    }

    @Override
    public Path toPath(String fileName) {
        return dataFilePathFactory.newPathFromName(fileName);
    }

    @Override
    public Path newPath() {
        String name = INDEX_PREFIX + uuid + "-" + indexFileCount.getAndIncrement();
        return dataFilePathFactory.newPathFromName(name);
    }

    @Override
    public Path toPath(IndexFileMeta file) {
        return Optional.ofNullable(file.externalPath())
                .map(Path::new)
                .orElse(new Path(dataFilePathFactory.parent(), file.fileName()));
    }

    @Override
    public boolean isExternalPath() {
        return dataFilePathFactory.isExternalPath();
    }
}
