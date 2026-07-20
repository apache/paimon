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

package org.apache.paimon.blob;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ManagedBlobReferenceCollector}. */
class ManagedBlobReferenceCollectorTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testCollectExactManagedReferencesFromFinalRows() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        Path dataFile = new Path(bucketPath, "data-a.avro");
        Path managedBlob = new Path(bucketPath, "data-b.managed.blob");
        Path externalBlob = new Path(tempDir.resolve("external/data-c.managed.blob").toUri());
        ManagedBlobReferenceCollector collector =
                new ManagedBlobReferenceCollector(
                        fileIO,
                        dataFile,
                        RowType.of(DataTypes.INT(), DataTypes.BLOB()),
                        Collections.singleton("f1"));

        collector.write(keyValue(RowKind.INSERT, Blob.fromFile(fileIO, managedBlob.toString())));
        collector.write(
                keyValue(RowKind.UPDATE_AFTER, Blob.fromFile(fileIO, managedBlob.toString())));
        collector.write(keyValue(RowKind.INSERT, Blob.fromFile(fileIO, externalBlob.toString())));
        collector.write(keyValue(RowKind.DELETE, Blob.fromFile(fileIO, managedBlob.toString())));
        collector.close();

        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        assertThat(collector.result()).isEqualTo(sidecar.getName());
        assertThat(ManagedBlobReferenceFile.read(fileIO, sidecar))
                .containsExactly(
                        new ManagedBlobReferenceFile.Reference(
                                bucketPath.toString(), managedBlob.getName()),
                        new ManagedBlobReferenceFile.Reference(
                                externalBlob.getParent().toString(), externalBlob.getName()));
    }

    @Test
    void testCollectManagedReferencesFromBlobArray() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        Path otherBucketPath = new Path(tempDir.resolve("bucket-1").toUri());
        fileIO.mkdirs(bucketPath);
        Path dataFile = new Path(bucketPath, "data-a.avro");
        Path first = new Path(bucketPath, "data-b.managed.blob");
        Path second = new Path(otherBucketPath, "data-c.managed.blob");
        Path external = new Path(tempDir.resolve("external/data-d.blob").toUri());
        ManagedBlobReferenceCollector collector =
                new ManagedBlobReferenceCollector(
                        fileIO,
                        dataFile,
                        RowType.of(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BLOB())),
                        Collections.singleton("f1"));

        collector.write(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(
                                        1,
                                        new GenericArray(
                                                new Object[] {
                                                    Blob.fromFile(fileIO, first.toString()),
                                                    null,
                                                    Blob.fromFile(fileIO, external.toString()),
                                                    Blob.fromFile(fileIO, second.toString())
                                                }))));
        collector.close();

        assertThat(
                        ManagedBlobReferenceFile.read(
                                fileIO, ManagedBlobReferenceFile.sidecarPath(dataFile)))
                .containsExactly(
                        new ManagedBlobReferenceFile.Reference(
                                bucketPath.toString(), first.getName()),
                        new ManagedBlobReferenceFile.Reference(
                                otherBucketPath.toString(), second.getName()));
    }

    @Test
    void testCollectsOnlyDeclaredFields() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        Path dataFile = new Path(bucketPath, "data-a.avro");
        Path managed = new Path(bucketPath, "data-b.managed.blob");
        Path unmanaged = new Path(bucketPath, "data-c.managed.blob");
        RowType valueType =
                RowType.of(
                        new org.apache.paimon.types.DataType[] {
                            DataTypes.INT(), DataTypes.BLOB(), DataTypes.BLOB()
                        },
                        new String[] {"id", "managed", "unmanaged"});
        ManagedBlobReferenceCollector collector =
                new ManagedBlobReferenceCollector(
                        fileIO, dataFile, valueType, Collections.singleton("managed"));

        collector.write(
                new KeyValue()
                        .replace(
                                GenericRow.of(1),
                                RowKind.INSERT,
                                GenericRow.of(
                                        1,
                                        Blob.fromFile(fileIO, managed.toString()),
                                        Blob.fromFile(fileIO, unmanaged.toString()))));
        collector.close();

        assertThat(
                        ManagedBlobReferenceFile.read(
                                fileIO, ManagedBlobReferenceFile.sidecarPath(dataFile)))
                .containsExactly(
                        new ManagedBlobReferenceFile.Reference(
                                bucketPath.toString(), managed.getName()));
    }

    @Test
    void testRejectsManagedMapBlobField() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data-a.avro").toUri());

        assertThatThrownBy(
                        () ->
                                new ManagedBlobReferenceCollector(
                                        fileIO,
                                        dataFile,
                                        RowType.of(
                                                DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                                        Collections.singleton("f0")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Primary-key managed MAP<X, BLOB> field 'f0' is not supported.");
    }

    private KeyValue keyValue(RowKind kind, Blob blob) {
        return new KeyValue().replace(GenericRow.of(1), kind, GenericRow.of(1, blob));
    }
}
