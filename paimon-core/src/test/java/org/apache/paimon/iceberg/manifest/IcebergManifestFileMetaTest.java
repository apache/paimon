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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the file and row counts of {@link IcebergManifestFileMeta}. */
class IcebergManifestFileMetaTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testLiveCountsExcludeDeletedEntries() throws Exception {
        FileStoreTable table = createTable();
        IcebergManifestFile manifestFile =
                IcebergManifestFile.create(
                        table, new IcebergPathFactory(new Path(table.location(), "metadata")));
        List<IcebergManifestEntry> entries =
                Arrays.asList(
                        entry(IcebergManifestEntry.Status.ADDED, "added-1.parquet", 1),
                        entry(IcebergManifestEntry.Status.ADDED, "added-2.parquet", 2),
                        entry(IcebergManifestEntry.Status.EXISTING, "existing.parquet", 4),
                        entry(IcebergManifestEntry.Status.DELETED, "deleted-1.parquet", 8),
                        entry(IcebergManifestEntry.Status.DELETED, "deleted-2.parquet", 16));

        List<IcebergManifestFileMeta> metas = manifestFile.rollingWrite(entries.iterator(), 2);

        assertThat(metas).hasSize(1);
        IcebergManifestFileMeta meta = metas.get(0);
        assertThat(meta.addedFilesCount()).isEqualTo(2);
        assertThat(meta.existingFilesCount()).isEqualTo(1);
        assertThat(meta.deletedFilesCount()).isEqualTo(2);
        assertThat(meta.liveFilesCount()).isEqualTo(3);
        assertThat(meta.liveRowsCount()).isEqualTo(7);
    }

    private static IcebergManifestEntry entry(
            IcebergManifestEntry.Status status, String path, long recordCount) {
        GenericMap emptyMap = new GenericMap(Collections.emptyMap());
        IcebergDataFileMeta file =
                new IcebergDataFileMeta(
                        IcebergDataFileMeta.Content.DATA,
                        path,
                        "parquet",
                        BinaryRow.EMPTY_ROW,
                        recordCount,
                        100,
                        emptyMap,
                        emptyMap,
                        emptyMap);
        return new IcebergManifestEntry(status, 1, 1, 1, file);
    }

    private FileStoreTable createTable() throws Exception {
        Schema schema =
                new Schema(
                        DataTypes.ROW(DataTypes.INT()).getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.toUri());
        new FileSystemSchemaManager(fileIO, path).createTable(schema);
        return FileStoreTableFactory.create(fileIO, path);
    }
}
