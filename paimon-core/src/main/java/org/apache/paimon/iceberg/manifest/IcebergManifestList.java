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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.manifest.IcebergManifestFile.CorruptManifestException;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This file includes several Iceberg {@link IcebergManifestFileMeta}s, representing the additional
 * changes since last snapshot.
 */
public class IcebergManifestList extends ObjectsFile<IcebergManifestFileMeta> {

    private final FormatReaderFactory readerFactory;

    public IcebergManifestList(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType manifestType,
            String compression,
            PathFactory pathFactory) {
        this(
                fileIO,
                fileFormat,
                manifestType,
                fileFormat.createReaderFactory(manifestType, manifestType, new ArrayList<>()),
                compression,
                pathFactory);
    }

    private IcebergManifestList(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType manifestType,
            FormatReaderFactory readerFactory,
            String compression,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestFileMetaSerializer(manifestType),
                manifestType,
                readerFactory,
                fileFormat.createWriterFactory(manifestType),
                compression,
                pathFactory,
                null);
        this.readerFactory = readerFactory;
    }

    @VisibleForTesting
    public String compression() {
        return compression;
    }

    /**
     * Reads a list, and tells a failure of the storage from a list that is damaged. A list that the
     * storage delivered whole and that does not decode, which stays the same however often it is
     * read, fails with a {@link CorruptManifestException}. A failing read, and a read that ended
     * before the end of the file, fail with an {@link IOException}, because the next read can
     * succeed.
     *
     * @param fileSize the size of the file, which is how a read that ended early is told
     */
    public List<IcebergManifestFileMeta> decode(String fileName, long fileSize) throws IOException {
        Path path = pathFactory.toPath(fileName);
        ReadWatch watch = new ReadWatch(fileIO, fileSize);
        List<IcebergManifestFileMeta> metas = new ArrayList<>();
        try (CloseableIterator<InternalRow> iterator =
                FileUtils.createFormatReader(watch, readerFactory, path, fileSize)
                        .toCloseableIterator()) {
            while (iterator.hasNext()) {
                metas.add(serializer.fromRow(iterator.next()));
            }
        } catch (Exception e) {
            watch.throwIfStorageFailed(path, e);
            throw new CorruptManifestException(path + " cannot be decoded: " + e, e);
        }
        watch.throwIfStorageFailed(path, null);
        return metas;
    }

    public static IcebergManifestList create(FileStoreTable table, IcebergPathFactory pathFactory) {
        Options avroOptions = Options.fromMap(table.options());
        // https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/ManifestLists.java
        avroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_file,"
                        + "iceberg:true,"
                        + "manifest_file_partitions:r508,"
                        + "array_id_r508:508");
        FileFormat fileFormat = FileFormat.manifestFormat(new CoreOptions(avroOptions));
        boolean withFirstRowId =
                avroOptions.get(IcebergOptions.FORMAT_VERSION) >= IcebergMetadata.FORMAT_VERSION_V3;
        RowType manifestType =
                IcebergManifestFileMeta.schema(
                        avroOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION), withFirstRowId);
        return new IcebergManifestList(
                table.fileIO(),
                fileFormat,
                manifestType,
                avroOptions.get(IcebergOptions.MANIFEST_COMPRESSION),
                pathFactory.manifestListFactory());
    }
}
