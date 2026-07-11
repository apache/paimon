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

/** Versioned metadata for an immutable ANN primary-key vector segment. */
public final class PkVectorAnnSegmentMeta {

    private static final int VERSION = 1;

    private final String indexType;
    private final List<PkVectorSourceFile> sourceFiles;
    private final byte[] payloadMetadata;

    public PkVectorAnnSegmentMeta(
            String indexType, List<PkVectorSourceFile> sourceFiles, byte[] payloadMetadata) {
        this.indexType = Objects.requireNonNull(indexType);
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        this.payloadMetadata = Arrays.copyOf(payloadMetadata, payloadMetadata.length);
        checkArgument(!this.sourceFiles.isEmpty(), "An ANN segment must reference source files.");
    }

    public String indexType() {
        return indexType;
    }

    public List<PkVectorSourceFile> sourceFiles() {
        return sourceFiles;
    }

    public byte[] payloadMetadata() {
        return Arrays.copyOf(payloadMetadata, payloadMetadata.length);
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(128);
            output.writeInt(VERSION);
            output.writeUTF(indexType);
            output.writeInt(sourceFiles.size());
            for (PkVectorSourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName());
                output.writeLong(sourceFile.rowCount());
            }
            output.writeInt(payloadMetadata.length);
            output.write(payloadMetadata);
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ANN vector segment metadata.", e);
        }
    }

    public static PkVectorAnnSegmentMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int version = input.readInt();
            checkArgument(
                    version == VERSION, "Unsupported ANN vector segment version: %s.", version);
            String indexType = input.readUTF();
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "An ANN segment must reference source files.");
            List<PkVectorSourceFile> sourceFiles = new ArrayList<>(sourceFileCount);
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(new PkVectorSourceFile(input.readUTF(), input.readLong()));
            }
            int payloadMetadataLength = input.readInt();
            checkArgument(
                    payloadMetadataLength >= 0, "Payload metadata length must not be negative.");
            byte[] payloadMetadata = new byte[payloadMetadataLength];
            input.readFully(payloadMetadata);
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in ANN vector segment metadata.");
            return new PkVectorAnnSegmentMeta(indexType, sourceFiles, payloadMetadata);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to deserialize ANN vector segment metadata.", e);
        }
    }
}
