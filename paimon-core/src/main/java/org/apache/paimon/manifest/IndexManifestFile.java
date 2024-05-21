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

package org.apache.paimon.manifest;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.List;

/** Index manifest file. */
public class IndexManifestFile extends ObjectsFile<IndexManifestEntry> {

    private IndexManifestFile(
            FileIO fileIO,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IndexManifestEntrySerializer(),
                readerFactory,
                writerFactory,
                pathFactory,
                null);
    }

    /** Write new index files to index manifest. */
    @Nullable
    public String writeIndexFiles(
            @Nullable String previousIndexManifest,
            List<IndexManifestEntry> newIndexFiles,
            BucketMode bucketMode) {
        if (newIndexFiles.isEmpty()) {
            return previousIndexManifest;
        }
        IndexManifestFileHandler handler = new IndexManifestFileHandler(this, bucketMode);
        return handler.write(previousIndexManifest, newIndexFiles);
    }

    /** Creator of {@link IndexManifestFile}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        public Factory(FileIO fileIO, FileFormat fileFormat, FileStorePathFactory pathFactory) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
        }

        public IndexManifestFile create() {
            RowType schema = VersionedObjectSerializer.versionType(IndexManifestEntry.schema());
            return new IndexManifestFile(
                    fileIO,
                    fileFormat.createReaderFactory(schema),
                    fileFormat.createWriterFactory(schema),
                    pathFactory.indexManifestFileFactory());
        }
    }
}
