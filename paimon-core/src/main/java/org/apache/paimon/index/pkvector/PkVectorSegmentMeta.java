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

package org.apache.paimon.index.pkvector;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Versioned metadata for an immutable primary-key vector segment. */
public class PkVectorSegmentMeta {

    private static final int VERSION = 1;

    private final Role role;
    private final String indexDefinitionId;
    private final int vectorFieldId;
    private final String vectorTypeFingerprint;
    private final String metric;
    private final String algorithm;
    private final List<SourceFile> sourceFiles;
    private final OrdinalLayout ordinalLayout;
    private final long liveRowCountAtBuild;
    private final long buildSnapshotId;
    private final byte[] optionsHash;
    private final byte[] payloadMetadata;

    public PkVectorSegmentMeta(
            Role role,
            String indexDefinitionId,
            int vectorFieldId,
            String vectorTypeFingerprint,
            String metric,
            String algorithm,
            List<SourceFile> sourceFiles,
            OrdinalLayout ordinalLayout,
            long liveRowCountAtBuild,
            long buildSnapshotId,
            byte[] optionsHash) {
        this(
                role,
                indexDefinitionId,
                vectorFieldId,
                vectorTypeFingerprint,
                metric,
                algorithm,
                sourceFiles,
                ordinalLayout,
                liveRowCountAtBuild,
                buildSnapshotId,
                optionsHash,
                new byte[0]);
    }

    public PkVectorSegmentMeta(
            Role role,
            String indexDefinitionId,
            int vectorFieldId,
            String vectorTypeFingerprint,
            String metric,
            String algorithm,
            List<SourceFile> sourceFiles,
            OrdinalLayout ordinalLayout,
            long liveRowCountAtBuild,
            long buildSnapshotId,
            byte[] optionsHash,
            byte[] payloadMetadata) {
        this.role = Objects.requireNonNull(role);
        this.indexDefinitionId = Objects.requireNonNull(indexDefinitionId);
        this.vectorFieldId = vectorFieldId;
        this.vectorTypeFingerprint = Objects.requireNonNull(vectorTypeFingerprint);
        this.metric = Objects.requireNonNull(metric);
        this.algorithm = Objects.requireNonNull(algorithm);
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        this.ordinalLayout = Objects.requireNonNull(ordinalLayout);
        this.liveRowCountAtBuild = liveRowCountAtBuild;
        this.buildSnapshotId = buildSnapshotId;
        this.optionsHash = Arrays.copyOf(optionsHash, optionsHash.length);
        this.payloadMetadata = Arrays.copyOf(payloadMetadata, payloadMetadata.length);

        checkArgument(!this.sourceFiles.isEmpty(), "A vector segment must reference source files.");
        checkArgument(liveRowCountAtBuild >= 0, "Live row count must not be negative.");
        checkArgument(buildSnapshotId >= 0, "Build snapshot id must not be negative.");
    }

    public Role role() {
        return role;
    }

    public String indexDefinitionId() {
        return indexDefinitionId;
    }

    public int vectorFieldId() {
        return vectorFieldId;
    }

    public String vectorTypeFingerprint() {
        return vectorTypeFingerprint;
    }

    public String metric() {
        return metric;
    }

    public String algorithm() {
        return algorithm;
    }

    public List<SourceFile> sourceFiles() {
        return sourceFiles;
    }

    public OrdinalLayout ordinalLayout() {
        return ordinalLayout;
    }

    public long liveRowCountAtBuild() {
        return liveRowCountAtBuild;
    }

    public long buildSnapshotId() {
        return buildSnapshotId;
    }

    public byte[] optionsHash() {
        return Arrays.copyOf(optionsHash, optionsHash.length);
    }

    public byte[] payloadMetadata() {
        return Arrays.copyOf(payloadMetadata, payloadMetadata.length);
    }

    /** Serializes this metadata for {@link org.apache.paimon.index.GlobalIndexMeta#indexMeta()}. */
    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(128);
            output.writeInt(VERSION);
            output.writeByte(role.ordinal());
            output.writeUTF(indexDefinitionId);
            output.writeInt(vectorFieldId);
            output.writeUTF(vectorTypeFingerprint);
            output.writeUTF(metric);
            output.writeUTF(algorithm);
            output.writeInt(sourceFiles.size());
            for (SourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName);
                output.writeLong(sourceFile.schemaId);
                output.writeInt(sourceFile.level);
                output.writeLong(sourceFile.rowCount);
                output.writeLong(sourceFile.fileSize);
            }
            output.writeByte(ordinalLayout.ordinal());
            output.writeLong(liveRowCountAtBuild);
            output.writeLong(buildSnapshotId);
            output.writeInt(optionsHash.length);
            output.write(optionsHash);
            output.writeInt(payloadMetadata.length);
            output.write(payloadMetadata);
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to serialize primary-key vector segment metadata.", e);
        }
    }

    /** Deserializes primary-key vector metadata stored in {@code GlobalIndexMeta.indexMeta}. */
    public static PkVectorSegmentMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int version = input.readInt();
            checkArgument(
                    version == VERSION,
                    "Unsupported primary-key vector segment version: %s.",
                    version);
            Role role = enumValue(Role.values(), input.readByte(), "role");
            String indexDefinitionId = input.readUTF();
            int vectorFieldId = input.readInt();
            String vectorTypeFingerprint = input.readUTF();
            String metric = input.readUTF();
            String algorithm = input.readUTF();
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "A vector segment must reference source files.");
            List<SourceFile> sourceFiles = new ArrayList<>(sourceFileCount);
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(
                        new SourceFile(
                                input.readUTF(),
                                input.readLong(),
                                input.readInt(),
                                input.readLong(),
                                input.readLong()));
            }
            OrdinalLayout ordinalLayout =
                    enumValue(OrdinalLayout.values(), input.readByte(), "ordinal layout");
            long liveRowCountAtBuild = input.readLong();
            long buildSnapshotId = input.readLong();
            int optionsHashLength = input.readInt();
            checkArgument(optionsHashLength >= 0, "Options hash length must not be negative.");
            byte[] optionsHash = new byte[optionsHashLength];
            input.readFully(optionsHash);
            int payloadMetadataLength = input.readInt();
            checkArgument(
                    payloadMetadataLength >= 0, "Payload metadata length must not be negative.");
            byte[] payloadMetadata = new byte[payloadMetadataLength];
            input.readFully(payloadMetadata);
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in vector segment metadata.");
            return new PkVectorSegmentMeta(
                    role,
                    indexDefinitionId,
                    vectorFieldId,
                    vectorTypeFingerprint,
                    metric,
                    algorithm,
                    sourceFiles,
                    ordinalLayout,
                    liveRowCountAtBuild,
                    buildSnapshotId,
                    optionsHash,
                    payloadMetadata);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to deserialize primary-key vector segment metadata.", e);
        }
    }

    private static <T> T enumValue(T[] values, byte ordinal, String field) {
        int index = ordinal;
        checkArgument(
                index >= 0 && index < values.length,
                "Unknown vector segment %s: %s.",
                field,
                ordinal);
        return values[index];
    }

    /** Role of an immutable vector payload. */
    public enum Role {
        RAW_DELTA,
        ANN
    }

    /** Mapping from a segment-local ordinal to a physical data-file position. */
    public enum OrdinalLayout {
        ROW_POSITION,
        FILE_POSITION
    }

    /** Immutable source data-file identity captured when a vector segment is built. */
    public static class SourceFile {

        private final String fileName;
        private final long schemaId;
        private final int level;
        private final long rowCount;
        private final long fileSize;

        public SourceFile(String fileName, long schemaId, int level, long rowCount, long fileSize) {
            this.fileName = Objects.requireNonNull(fileName);
            this.schemaId = schemaId;
            this.level = level;
            this.rowCount = rowCount;
            this.fileSize = fileSize;
            checkArgument(rowCount >= 0, "Source file row count must not be negative.");
            checkArgument(fileSize >= 0, "Source file size must not be negative.");
        }

        public String fileName() {
            return fileName;
        }

        public long schemaId() {
            return schemaId;
        }

        public int level() {
            return level;
        }

        public long rowCount() {
            return rowCount;
        }

        public long fileSize() {
            return fileSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SourceFile that = (SourceFile) o;
            return schemaId == that.schemaId
                    && level == that.level
                    && rowCount == that.rowCount
                    && fileSize == that.fileSize
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, schemaId, level, rowCount, fileSize);
        }
    }
}
