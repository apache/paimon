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

    private final String indexDefinitionId;
    private final List<SourceFile> sourceFiles;
    private final OrdinalLayout ordinalLayout;
    private final byte[] payloadMetadata;

    public PkVectorSegmentMeta(
            String indexDefinitionId,
            List<SourceFile> sourceFiles,
            OrdinalLayout ordinalLayout,
            byte[] payloadMetadata) {
        this.indexDefinitionId = Objects.requireNonNull(indexDefinitionId);
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        this.ordinalLayout = Objects.requireNonNull(ordinalLayout);
        this.payloadMetadata = Arrays.copyOf(payloadMetadata, payloadMetadata.length);

        checkArgument(!this.sourceFiles.isEmpty(), "A vector segment must reference source files.");
    }

    public String indexDefinitionId() {
        return indexDefinitionId;
    }

    public List<SourceFile> sourceFiles() {
        return sourceFiles;
    }

    public OrdinalLayout ordinalLayout() {
        return ordinalLayout;
    }

    public byte[] payloadMetadata() {
        return Arrays.copyOf(payloadMetadata, payloadMetadata.length);
    }

    /** Serializes this metadata for {@link org.apache.paimon.index.GlobalIndexMeta#indexMeta()}. */
    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(128);
            output.writeInt(VERSION);
            output.writeUTF(indexDefinitionId);
            output.writeInt(sourceFiles.size());
            for (SourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName);
                output.writeLong(sourceFile.rowCount);
            }
            output.writeByte(ordinalLayout.ordinal());
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
            String indexDefinitionId = input.readUTF();
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "A vector segment must reference source files.");
            List<SourceFile> sourceFiles = new ArrayList<>(sourceFileCount);
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(new SourceFile(input.readUTF(), input.readLong()));
            }
            OrdinalLayout ordinalLayout =
                    enumValue(OrdinalLayout.values(), input.readByte(), "ordinal layout");
            int payloadMetadataLength = input.readInt();
            checkArgument(
                    payloadMetadataLength >= 0, "Payload metadata length must not be negative.");
            byte[] payloadMetadata = new byte[payloadMetadataLength];
            input.readFully(payloadMetadata);
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in vector segment metadata.");
            return new PkVectorSegmentMeta(
                    indexDefinitionId, sourceFiles, ordinalLayout, payloadMetadata);
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

    /** Mapping from a segment-local ordinal to a physical data-file position. */
    public enum OrdinalLayout {
        ROW_POSITION,
        FILE_POSITION
    }

    /** Immutable source data-file identity captured when a vector segment is built. */
    public static class SourceFile {

        private final String fileName;
        private final long rowCount;

        public SourceFile(String fileName, long rowCount) {
            this.fileName = Objects.requireNonNull(fileName);
            this.rowCount = rowCount;
            checkArgument(rowCount >= 0, "Source file row count must not be negative.");
        }

        public String fileName() {
            return fileName;
        }

        public long rowCount() {
            return rowCount;
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
            return rowCount == that.rowCount && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileName, rowCount);
        }
    }
}
