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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.blob.ManagedBlobReferenceFile;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FlushingFileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for managed BLOB references produced by {@link KeyValueFileWriterFactory}. */
class PrimaryKeyBlobFileWriterTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testAttachReferenceSidecarToDataFile() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(tempDir.toUri());
        RowType keyType =
                new RowType(
                        Collections.singletonList(
                                new DataField(
                                        SpecialFields.KEY_FIELD_ID_START,
                                        SpecialFields.KEY_FIELD_PREFIX + "id",
                                        DataTypes.INT())));
        RowType valueType =
                new RowType(
                        java.util.Arrays.asList(
                                new DataField(0, "id", DataTypes.INT()),
                                new DataField(1, "payload", DataTypes.BLOB())));
        Function<String, FileStorePathFactory> pathFactories =
                format ->
                        new FileStorePathFactory(
                                tablePath,
                                RowType.of(),
                                CoreOptions.PARTITION_DEFAULT_NAME.defaultValue(),
                                format,
                                CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                                CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                                CoreOptions.PARTITION_GENERATE_LEGACY_NAME.defaultValue(),
                                CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                                CoreOptions.FILE_COMPRESSION.defaultValue(),
                                null,
                                null,
                                CoreOptions.ExternalPathStrategy.NONE,
                                null,
                                false,
                                null);
        Options options = new Options();
        options.set(CoreOptions.BLOB_DESCRIPTOR_FIELD, "payload");
        KeyValueFileWriterFactory factory =
                KeyValueFileWriterFactory.builder(
                                fileIO,
                                0,
                                keyType,
                                valueType,
                                new FlushingFileFormat("avro"),
                                pathFactories,
                                1024 * 1024)
                        .build(BinaryRow.EMPTY_ROW, 0, new CoreOptions(options));
        DataFilePathFactory pathFactory = factory.pathFactory(0);
        Path managedBlob =
                pathFactory.newPathFromExtension(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                factory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);

        writer.write(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                0,
                                RowKind.INSERT,
                                GenericRow.of(1, Blob.fromFile(fileIO, managedBlob.toString()))));
        writer.close();

        DataFileMeta meta = writer.result().get(0);
        assertThat(meta.extraFiles()).singleElement().asString().endsWith(".blobref");
        Path sidecar = pathFactory.toAlignedPath(meta.extraFiles().get(0), meta);
        assertThat(ManagedBlobReferenceFile.read(fileIO, sidecar))
                .containsExactly(
                        new ManagedBlobReferenceFile.Reference(
                                sidecar.getParent().toString(), managedBlob.getName()));

        Path dataPath = pathFactory.toPath(meta);
        factory.deleteFile(meta);
        assertThat(fileIO.exists(dataPath)).isFalse();
        assertThat(fileIO.exists(sidecar)).isFalse();

        RollingFileWriter<KeyValue, DataFileMeta> emptyWriter =
                factory.createRollingMergeTreeFileWriter(0, FileSource.APPEND);
        emptyWriter.write(
                new KeyValue()
                        .replace(GenericRow.of(2), 1, RowKind.INSERT, GenericRow.of(2, null)));
        emptyWriter.close();

        DataFileMeta emptyMeta = emptyWriter.result().get(0);
        assertThat(emptyMeta.extraFiles()).singleElement().asString().endsWith(".blobref");
        Path emptySidecar = pathFactory.toAlignedPath(emptyMeta.extraFiles().get(0), emptyMeta);
        assertThat(ManagedBlobReferenceFile.read(fileIO, emptySidecar)).isEmpty();
    }
}
