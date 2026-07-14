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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Versioned metadata containing the managed BLOB packs referenced by one data file. */
public class ManagedBlobReferenceFile {

    private static final int MAGIC = 0x50424C52;
    private static final int VERSION = 1;

    public static final String MANAGED_BLOB_SUFFIX = ".managed.blob";
    public static final String REFERENCE_FILE_SUFFIX = ".blobref";

    private ManagedBlobReferenceFile() {}

    public static Optional<Reference> fromDescriptorUri(Path dataFile, String descriptorUri) {
        Path blobPath = new Path(descriptorUri);
        if (!blobPath.getName().endsWith(MANAGED_BLOB_SUFFIX)
                || !dataFile.getParent().equals(blobPath.getParent())) {
            return Optional.empty();
        }
        return Optional.of(new Reference(blobPath.getParent().toString(), blobPath.getName()));
    }

    public static Path sidecarPath(Path dataFile) {
        return new Path(dataFile.getParent(), dataFile.getName() + REFERENCE_FILE_SUFFIX);
    }

    public static void write(FileIO fileIO, Path path, List<Reference> references)
            throws IOException {
        List<Reference> normalized = new ArrayList<>(new HashSet<>(references));
        normalized.sort(
                Comparator.comparing(Reference::storageRootId)
                        .thenComparing(Reference::relativePath));

        try (DataOutputStream out = new DataOutputStream(fileIO.newOutputStream(path, false))) {
            out.writeInt(MAGIC);
            CRC32 checksum = new CRC32();
            DataOutputStream payload = new DataOutputStream(new CheckedOutputStream(out, checksum));
            payload.writeByte(VERSION);
            payload.writeInt(normalized.size());
            for (Reference reference : normalized) {
                payload.writeUTF(reference.storageRootId());
                payload.writeUTF(reference.relativePath());
            }
            payload.flush();
            out.writeInt((int) checksum.getValue());
        } catch (IOException e) {
            fileIO.deleteQuietly(path);
            throw e;
        }
    }

    public static List<Reference> read(FileIO fileIO, Path path) throws IOException {
        try (DataInputStream in = new DataInputStream(fileIO.newInputStream(path))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid managed BLOB reference file magic: " + magic);
            }

            CRC32 checksum = new CRC32();
            DataInputStream payload = new DataInputStream(new CheckedInputStream(in, checksum));
            int version = payload.readUnsignedByte();
            if (version != VERSION) {
                throw new IOException(
                        "Unsupported managed BLOB reference file version: " + version);
            }

            int count = payload.readInt();
            if (count < 0) {
                throw new IOException("Invalid managed BLOB reference count: " + count);
            }

            List<Reference> references = new ArrayList<>(Math.min(count, 1024));
            for (int i = 0; i < count; i++) {
                String storageRootId = payload.readUTF();
                String relativePath = payload.readUTF();
                try {
                    references.add(new Reference(storageRootId, relativePath));
                } catch (IllegalArgumentException e) {
                    throw new IOException("Invalid managed BLOB reference entry.", e);
                }
            }

            int actualChecksum = (int) checksum.getValue();
            int expectedChecksum = payload.readInt();
            if (expectedChecksum != actualChecksum) {
                throw new IOException(
                        "Invalid managed BLOB reference file checksum. Expected "
                                + Integer.toUnsignedLong(expectedChecksum)
                                + " but computed "
                                + Integer.toUnsignedLong(actualChecksum)
                                + ".");
            }
            if (payload.read() != -1) {
                throw new IOException("Unexpected trailing bytes in managed BLOB reference file.");
            }
            return references;
        }
    }

    /** Exact identity of a managed BLOB payload pack. */
    public static class Reference {

        private final String storageRootId;
        private final String relativePath;

        public Reference(String storageRootId, String relativePath) {
            checkArgument(
                    storageRootId != null && !storageRootId.isEmpty(),
                    "Managed BLOB storage root must not be empty.");
            checkArgument(
                    relativePath != null
                            && !relativePath.isEmpty()
                            && relativePath.equals(new Path(relativePath).getName())
                            && !".".equals(relativePath)
                            && !"..".equals(relativePath),
                    "Managed BLOB relative path must be a file name: %s.",
                    relativePath);
            this.storageRootId = storageRootId;
            this.relativePath = relativePath;
        }

        public String storageRootId() {
            return storageRootId;
        }

        public String relativePath() {
            return relativePath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Reference reference = (Reference) o;
            return Objects.equals(storageRootId, reference.storageRootId)
                    && Objects.equals(relativePath, reference.relativePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(storageRootId, relativePath);
        }

        @Override
        public String toString() {
            return storageRootId + "/" + relativePath;
        }
    }
}
