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

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PrimaryKeyBlobExternalizer}. */
class PrimaryKeyBlobExternalizerTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testExternalizeRawBlobBeforeBuffering() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        RowType valueType = RowType.of(DataTypes.INT(), DataTypes.BLOB());
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(fileIO, valueType, pathFactory, 1024L);
        byte[] expected = "managed-blob".getBytes(StandardCharsets.UTF_8);

        InternalRow result =
                externalizer.externalize(RowKind.INSERT, GenericRow.of(1, Blob.fromData(expected)));
        Blob blob = result.getBlob(1);

        assertThat(blob).isInstanceOf(BlobRef.class);
        assertThat(blob.toDescriptor().uri())
                .endsWith(ManagedBlobReferenceFile.MANAGED_BLOB_SUFFIX);

        externalizer.prepareCommit();

        assertThat(blob.toData()).isEqualTo(expected);
        assertThat(fileIO.exists(new Path(blob.toDescriptor().uri()))).isTrue();
    }

    @Test
    void testRetractSkipsPayloadAndAbortDeletesPrivatePack() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path bucketPath = new Path(tempDir.resolve("bucket-0").toUri());
        fileIO.mkdirs(bucketPath);
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        bucketPath, "avro", "data-", "changelog-", false, null, null);
        PrimaryKeyBlobExternalizer externalizer =
                new PrimaryKeyBlobExternalizer(
                        fileIO, RowType.of(DataTypes.INT(), DataTypes.BLOB()), pathFactory, 1024L);

        InternalRow retract =
                externalizer.externalize(
                        RowKind.DELETE, GenericRow.of(1, Blob.fromData(new byte[] {1})));
        assertThat(retract.isNullAt(1)).isTrue();
        assertThat(fileIO.listStatus(bucketPath)).isEmpty();

        Blob privateBlob =
                externalizer
                        .externalize(
                                RowKind.INSERT, GenericRow.of(2, Blob.fromData(new byte[] {2})))
                        .getBlob(1);
        Path privatePack = new Path(privateBlob.toDescriptor().uri());
        assertThat(fileIO.exists(privatePack)).isTrue();

        externalizer.abort();

        assertThat(fileIO.exists(privatePack)).isFalse();
    }
}
