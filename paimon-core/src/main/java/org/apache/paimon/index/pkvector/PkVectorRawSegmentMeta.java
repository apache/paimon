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
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Versioned metadata for an immutable RAW primary-key vector segment. */
public final class PkVectorRawSegmentMeta {

    private static final int VERSION = 1;

    private final String indexDefinitionId;
    private final PkVectorSourceFile sourceFile;

    public PkVectorRawSegmentMeta(String indexDefinitionId, PkVectorSourceFile sourceFile) {
        this.indexDefinitionId = Objects.requireNonNull(indexDefinitionId);
        this.sourceFile = Objects.requireNonNull(sourceFile);
    }

    public String indexDefinitionId() {
        return indexDefinitionId;
    }

    public PkVectorSourceFile sourceFile() {
        return sourceFile;
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(64);
            output.writeInt(VERSION);
            output.writeUTF(indexDefinitionId);
            output.writeUTF(sourceFile.fileName());
            output.writeLong(sourceFile.rowCount());
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize RAW vector segment metadata.", e);
        }
    }

    public static PkVectorRawSegmentMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int version = input.readInt();
            checkArgument(
                    version == VERSION, "Unsupported RAW vector segment version: %s.", version);
            String indexDefinitionId = input.readUTF();
            PkVectorSourceFile sourceFile =
                    new PkVectorSourceFile(input.readUTF(), input.readLong());
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in RAW vector segment metadata.");
            return new PkVectorRawSegmentMeta(indexDefinitionId, sourceFile);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to deserialize RAW vector segment metadata.", e);
        }
    }
}
