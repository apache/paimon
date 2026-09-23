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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ManagedBlobReferenceFile}. */
class ManagedBlobReferenceFileTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testRoundTripAndDeduplicateReferences() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.resolve("data.avro.blobref").toUri());
        ManagedBlobReferenceFile.Reference first =
                new ManagedBlobReferenceFile.Reference(
                        tempDir.resolve("bucket-0").toUri().toString(), "data-a.managed.blob");
        ManagedBlobReferenceFile.Reference second =
                new ManagedBlobReferenceFile.Reference(
                        tempDir.resolve("bucket-0").toUri().toString(), "data-b.managed.blob");

        ManagedBlobReferenceFile.write(fileIO, path, Arrays.asList(second, first, second));

        assertThat(ManagedBlobReferenceFile.read(fileIO, path)).containsExactly(first, second);

        Path emptyPath = new Path(tempDir.resolve("empty.avro.blobref").toUri());
        ManagedBlobReferenceFile.write(fileIO, emptyPath, Collections.emptyList());
        assertThat(ManagedBlobReferenceFile.read(fileIO, emptyPath)).isEmpty();
    }

    @Test
    void testClassifyManagedBlobPathAcrossDirectories() {
        Path dataFile = new Path(tempDir.resolve("bucket-0/data-a.avro").toUri());
        Path managedBlob = new Path(tempDir.resolve("bucket-0/data-b.managed.blob").toUri());
        Path externalManagedBlob =
                new Path(tempDir.resolve("external/data-c.managed.blob").toUri());
        Path ordinaryBlob = new Path(tempDir.resolve("bucket-0/data-d.blob").toUri());

        assertThat(ManagedBlobReferenceFile.fromDescriptorUri(managedBlob.toString()))
                .contains(
                        new ManagedBlobReferenceFile.Reference(
                                dataFile.getParent().toString(), managedBlob.getName()));
        assertThat(ManagedBlobReferenceFile.fromDescriptorUri(externalManagedBlob.toString()))
                .contains(
                        new ManagedBlobReferenceFile.Reference(
                                externalManagedBlob.getParent().toString(),
                                externalManagedBlob.getName()));
        assertThat(ManagedBlobReferenceFile.fromDescriptorUri(ordinaryBlob.toString())).isEmpty();
        assertThat(ManagedBlobReferenceFile.sidecarPath(dataFile).getName())
                .isEqualTo("data-a.avro.blobref");
    }

    @Test
    void testRejectUnsupportedVersion() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.resolve("unsupported.avro.blobref").toUri());
        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(path, false))) {
            out.writeInt(0x50424C52);
            out.writeByte(99);
            out.writeInt(0);
        }

        assertThatThrownBy(() -> ManagedBlobReferenceFile.read(fileIO, path))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Unsupported managed BLOB reference file version: 99");
    }

    @Test
    void testRejectCorruptChecksum() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path path = new Path(tempDir.resolve("corrupt.avro.blobref").toUri());
        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(path, false))) {
            out.writeInt(0x50424C52);
            out.writeByte(1);
            out.writeInt(0);
            out.writeInt(12345);
        }

        assertThatThrownBy(() -> ManagedBlobReferenceFile.read(fileIO, path))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Invalid managed BLOB reference file checksum");
    }
}
