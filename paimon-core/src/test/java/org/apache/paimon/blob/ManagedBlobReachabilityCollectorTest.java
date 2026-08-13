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

import org.apache.paimon.blob.ManagedBlobReferenceFile.Reference;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ManagedBlobReachabilityCollector}. */
class ManagedBlobReachabilityCollectorTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testEmptyExtraFiles() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.<String>emptyList());

        assertThat(result.isUnsafe()).isFalse();
        assertThat(result.referenced()).isEmpty();
    }

    @Test
    void testEmptySidecar() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        ManagedBlobReferenceFile.write(fileIO, sidecar, Collections.<Reference>emptyList());
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.singletonList(sidecar.getName()));

        assertThat(result.isUnsafe()).isFalse();
        assertThat(result.referenced()).isEmpty();
    }

    @Test
    void testReferencedPacks() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        Reference first =
                new Reference(tempDir.resolve("bucket-0").toUri().toString(), "data-a.managed.blob");
        Reference second =
                new Reference(tempDir.resolve("bucket-0").toUri().toString(), "data-b.managed.blob");
        ManagedBlobReferenceFile.write(fileIO, sidecar, Arrays.asList(first, second));
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.singletonList(sidecar.getName()));

        assertThat(result.isUnsafe()).isFalse();
        assertThat(result.referenced()).containsExactlyInAnyOrder(first, second);
        assertThat(result.contains(first)).isTrue();
        assertThat(result.containsPackName("data-b.managed.blob")).isTrue();
        assertThat(result.containsPackName("missing.managed.blob")).isFalse();
        assertThat(first.toPath())
                .isEqualTo(new Path(tempDir.resolve("bucket-0/data-a.managed.blob").toUri()));
    }

    @Test
    void testMissingSidecarUnsafeWhenDataFileExists() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        fileIO.newOutputStream(dataFile, false).close();
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.singletonList("data.avro.blobref"));

        assertThat(result.isUnsafe()).isTrue();
        assertThat(result.referenced()).isEmpty();
    }

    @Test
    void testMissingSidecarIgnoredWhenDataFileGone() {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("expired.avro").toUri());
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(
                        dataFile, Collections.singletonList("expired.avro.blobref"));

        assertThat(result.isUnsafe()).isFalse();
        assertThat(result.referenced()).isEmpty();
    }

    @Test
    void testCorruptSidecarUnsafe() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        fileIO.newOutputStream(dataFile, false).close();
        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(sidecar, false))) {
            out.writeInt(0x50424C52);
            out.writeByte(1);
            out.writeInt(0);
            out.writeInt(12345);
        }
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.singletonList(sidecar.getName()));

        assertThat(result.isUnsafe()).isTrue();
    }

    @Test
    void testUnsupportedVersionUnsafe() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        fileIO.newOutputStream(dataFile, false).close();
        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(sidecar, false))) {
            out.writeInt(0x50424C52);
            out.writeByte(99);
            out.writeInt(0);
        }
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result result =
                collector.fromDataFile(dataFile, Collections.singletonList(sidecar.getName()));

        assertThat(result.isUnsafe()).isTrue();
    }

    @Test
    void testMergePropagatesUnsafe() throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        Path dataFile = new Path(tempDir.resolve("data.avro").toUri());
        Path sidecar = ManagedBlobReferenceFile.sidecarPath(dataFile);
        Reference referenced =
                new Reference(tempDir.resolve("bucket-0").toUri().toString(), "data-a.managed.blob");
        ManagedBlobReferenceFile.write(fileIO, sidecar, Collections.singletonList(referenced));
        ManagedBlobReachabilityCollector collector = new ManagedBlobReachabilityCollector(fileIO);

        ManagedBlobReachabilityCollector.Result safe =
                collector.fromDataFile(dataFile, Collections.singletonList(sidecar.getName()));
        ManagedBlobReachabilityCollector.Result merged =
                safe.merge(ManagedBlobReachabilityCollector.Result.unsafe());

        assertThat(merged.isUnsafe()).isTrue();
        assertThat(merged.referenced()).containsExactly(referenced);
        assertThat(ManagedBlobReachabilityCollector.Result.empty()
                        .merge(ManagedBlobReachabilityCollector.Result.unsafe())
                        .isUnsafe())
                .isTrue();
    }
}
