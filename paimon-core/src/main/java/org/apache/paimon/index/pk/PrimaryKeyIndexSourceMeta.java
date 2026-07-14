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

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Ordered source data files covered by a source-backed primary-key index payload. */
public final class PrimaryKeyIndexSourceMeta {

    private static final int VERSION = 1;

    private final List<PrimaryKeyIndexSourceFile> sourceFiles;

    public PrimaryKeyIndexSourceMeta(List<PrimaryKeyIndexSourceFile> sourceFiles) {
        this.sourceFiles = Collections.unmodifiableList(new ArrayList<>(sourceFiles));
        checkArgument(!this.sourceFiles.isEmpty(), "An index must reference source files.");
    }

    public PrimaryKeyIndexSourceMeta(PrimaryKeyIndexSourceFile sourceFile) {
        this(Collections.singletonList(sourceFile));
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
            output.writeInt(VERSION);
            output.writeInt(sourceFiles.size());
            for (PrimaryKeyIndexSourceFile sourceFile : sourceFiles) {
                output.writeUTF(sourceFile.fileName());
                output.writeLong(sourceFile.rowCount());
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
            checkArgument(version == VERSION, "Unsupported index source version: %s.", version);
            int sourceFileCount = input.readInt();
            checkArgument(sourceFileCount > 0, "An index must reference source files.");
            List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>(sourceFileCount);
            for (int i = 0; i < sourceFileCount; i++) {
                sourceFiles.add(new PrimaryKeyIndexSourceFile(input.readUTF(), input.readLong()));
            }
            checkArgument(
                    input.available() == 0, "Unexpected trailing bytes in index source metadata.");
            return new PrimaryKeyIndexSourceMeta(sourceFiles);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize index source metadata.", e);
        }
    }
}
