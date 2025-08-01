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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

/**
 * This file includes several Iceberg {@link IcebergManifestFileMeta}s, representing the additional
 * changes since last snapshot.
 */
public class IcebergManifestList extends ObjectsFile<IcebergManifestFileMeta> {

    public IcebergManifestList(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType manifestType,
            String compression,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestFileMetaSerializer(manifestType),
                manifestType,
                fileFormat.createReaderFactory(manifestType),
                fileFormat.createWriterFactory(manifestType),
                compression,
                pathFactory,
                null);
    }

    @VisibleForTesting
    public String compression() {
        return compression;
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
        FileFormat fileFormat = FileFormat.fromIdentifier("avro", avroOptions);
        RowType manifestType =
                IcebergManifestFileMeta.schema(
                        avroOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION));
        return new IcebergManifestList(
                table.fileIO(),
                fileFormat,
                manifestType,
                avroOptions.get(IcebergOptions.MANIFEST_COMPRESSION),
                pathFactory.manifestListFactory());
    }
}
