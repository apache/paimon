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

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Ordered source data files for a primary-key vector index payload. */
public final class PkVectorSourceMeta {

    private static final int VERSION = 1;

    private final List<PkVectorSourceFile> sourceFiles;

    public PkVectorSourceMeta(List<PkVectorSourceFile> sourceFiles) {
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        checkArgument(!this.sourceFiles.isEmpty(), "A vector index must reference source files.");
    }

    public List<PkVectorSourceFile> sourceFiles() {
        return sourceFiles;
    }

    public static PkVectorSourceMeta fromIndexFile(IndexFileMeta indexFile) {
        GlobalIndexMeta globalIndexMeta = indexFile.globalIndexMeta();
        checkArgument(
                globalIndexMeta != null && globalIndexMeta.sourceMeta() != null,
                "Vector index file %s has no source metadata.",
                indexFile.fileName());
        return deserialize(globalIndexMeta.sourceMeta());
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(128);
            output.writeInt(VERSION);
            output.writeInt(sourceFiles.size());
            for (PkVectorSourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName());
                output.writeLong(sourceFile.rowCount());
            }
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize vector source metadata.", e);
        }
    }

    public static PkVectorSourceMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int version = input.readInt();
            checkArgument(version == VERSION, "Unsupported vector source version: %s.", version);
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "A vector index must reference source files.");
            List<PkVectorSourceFile> sourceFiles = new ArrayList<>(sourceFileCount);
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(new PkVectorSourceFile(input.readUTF(), input.readLong()));
            }
            checkArgument(
                    input.available() == 0, "Unexpected trailing bytes in vector source metadata.");
            return new PkVectorSourceMeta(sourceFiles);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize vector source metadata.", e);
        }
    }
}
