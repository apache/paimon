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

import org.apache.paimon.Snapshot;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This file includes several {@link ManifestFileMeta}, representing all data of the whole table at
 * the corresponding snapshot.
 */
public class ManifestList extends ObjectsFile<ManifestFileMeta> {

    private ManifestList(
            FileIO fileIO,
            ManifestFileMetaSerializer serializer,
            RowType schema,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                serializer,
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
    }

    /**
     * Return all {@link ManifestFileMeta} instances for either data or changelog manifests in this
     * snapshot.
     *
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> readAllManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(readDataManifests(snapshot));
        result.addAll(readChangelogManifests(snapshot));
        return result;
    }

    /**
     * Return a {@link ManifestFileMeta} for each data manifest in this snapshot.
     *
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> readDataManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(read(snapshot.baseManifestList()));
        result.addAll(readDeltaManifests(snapshot));
        return result;
    }

    /**
     * Return a {@link ManifestFileMeta} for each delta manifest in this snapshot.
     *
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> readDeltaManifests(Snapshot snapshot) {
        return read(snapshot.deltaManifestList());
    }

    /**
     * Return a {@link ManifestFileMeta} for each changelog manifest in this snapshot.
     *
     * @return a list of ManifestFileMeta.
     */
    public List<ManifestFileMeta> readChangelogManifests(Snapshot snapshot) {
        return snapshot.changelogManifestList() == null
                ? Collections.emptyList()
                : read(snapshot.changelogManifestList());
    }

    /**
     * Write several {@link ManifestFileMeta}s into a manifest list.
     *
     * <p>NOTE: This method is atomic.
     */
    public String write(List<ManifestFileMeta> metas) {
        return super.writeWithoutRolling(metas);
    }

    /** Creator of {@link ManifestList}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        @Nullable private final SegmentsCache<Path> cache;

        public Factory(
                FileIO fileIO,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.cache = cache;
        }

        public ManifestList create() {
            RowType metaType = VersionedObjectSerializer.versionType(ManifestFileMeta.SCHEMA);
            return new ManifestList(
                    fileIO,
                    new ManifestFileMetaSerializer(),
                    metaType,
                    fileFormat.createReaderFactory(metaType),
                    fileFormat.createWriterFactory(metaType),
                    compression,
                    pathFactory.manifestListFactory(),
                    cache);
        }
    }
}
