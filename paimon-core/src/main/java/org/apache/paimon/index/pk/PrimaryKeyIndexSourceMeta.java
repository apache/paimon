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

package org.apache.paimon.index.pk;

import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Ordered source data files covered by a source-backed primary-key index payload. */
public final class PrimaryKeyIndexSourceMeta {

    private static final int VERSION_1 = 1;
    private static final int VERSION_2 = 2;

    private final List<PrimaryKeyIndexSourceFile> sourceFiles;
    private final String definitionFingerprint;

    public PrimaryKeyIndexSourceMeta(List<PrimaryKeyIndexSourceFile> sourceFiles) {
        this(sourceFiles, null);
    }

    public PrimaryKeyIndexSourceMeta(
            List<PrimaryKeyIndexSourceFile> sourceFiles, String definitionFingerprint) {
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        checkArgument(!this.sourceFiles.isEmpty(), "An index must reference source files.");
        checkArgument(
                definitionFingerprint == null || !definitionFingerprint.trim().isEmpty(),
                "Definition fingerprint must not be empty.");
        this.definitionFingerprint = definitionFingerprint;
    }

    public PrimaryKeyIndexSourceMeta(PrimaryKeyIndexSourceFile sourceFile) {
        this(Collections.singletonList(sourceFile));
    }

    public PrimaryKeyIndexSourceMeta(
            PrimaryKeyIndexSourceFile sourceFile, String definitionFingerprint) {
        this(Collections.singletonList(sourceFile), definitionFingerprint);
    }

    public List<PrimaryKeyIndexSourceFile> sourceFiles() {
        return sourceFiles;
    }

    public PrimaryKeyIndexSourceFile sourceFile() {
        checkArgument(
                sourceFiles.size() == 1,
                "Expected exactly one source file, but found %s.",
                sourceFiles.size());
        return sourceFiles.get(0);
    }

    public Optional<String> definitionFingerprint() {
        return Optional.ofNullable(definitionFingerprint);
    }

    public static PrimaryKeyIndexSourceMeta fromIndexFile(IndexFileMeta indexFile) {
        GlobalIndexMeta globalIndexMeta = indexFile.globalIndexMeta();
        checkArgument(
                globalIndexMeta != null && globalIndexMeta.sourceMeta() != null,
                "Index file %s has no source metadata.",
                indexFile.fileName());
        return deserialize(globalIndexMeta.sourceMeta());
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(128);
            output.writeInt(definitionFingerprint == null ? VERSION_1 : VERSION_2);
            output.writeInt(sourceFiles.size());
            for (PrimaryKeyIndexSourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName());
                output.writeLong(sourceFile.rowCount());
            }
            if (definitionFingerprint != null) {
                output.writeUTF(definitionFingerprint);
            }
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize index source metadata.", e);
        }
    }

    public static PrimaryKeyIndexSourceMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int version = input.readInt();
            checkArgument(
                    version == VERSION_1 || version == VERSION_2,
                    "Unsupported index source version: %s.",
                    version);
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "An index must reference source files.");
            // Each entry needs at least the two-byte writeUTF length and one long.
            int maximumSourceFileCount = input.available() / (Short.BYTES + Long.BYTES);
            checkArgument(
                    sourceFileCount <= maximumSourceFileCount,
                    "Failed to deserialize index source metadata: source file count %s "
                            + "exceeds the maximum %s allowed by the remaining bytes.",
                    sourceFileCount,
                    maximumSourceFileCount);
            List<PrimaryKeyIndexSourceFile> sourceFiles =
                    new ArrayList<>(Math.min(sourceFileCount, 1024));
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(new PrimaryKeyIndexSourceFile(input.readUTF(), input.readLong()));
            }
            String definitionFingerprint = version == VERSION_2 ? input.readUTF() : null;
            checkArgument(
                    input.available() == 0, "Unexpected trailing bytes in index source metadata.");
            return new PrimaryKeyIndexSourceMeta(sourceFiles, definitionFingerprint);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize index source metadata.", e);
        }
    }
}
