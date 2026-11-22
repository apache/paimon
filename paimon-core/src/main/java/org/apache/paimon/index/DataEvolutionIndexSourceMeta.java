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

package org.apache.paimon.index;

import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Snapshot and table schema used to build a global index for a Data Evolution table. */
public final class DataEvolutionIndexSourceMeta {

    // "DEIX". The marker distinguishes this metadata from primary-key index source metadata.
    private static final int MAGIC = 0x44454958;
    private static final int VERSION = 1;
    private static final int VERSION_WITH_SCHEMA = 2;
    private static final int SNAPSHOT_ONLY_SIZE = 2 * Integer.BYTES + Long.BYTES;

    private final long scanSnapshotId;
    @Nullable private final Long sourceSchemaId;

    public DataEvolutionIndexSourceMeta(long scanSnapshotId) {
        this(scanSnapshotId, null);
    }

    public DataEvolutionIndexSourceMeta(long scanSnapshotId, @Nullable Long sourceSchemaId) {
        checkArgument(scanSnapshotId > 0, "Scan snapshot id must be positive.");
        checkArgument(
                sourceSchemaId == null || sourceSchemaId >= 0,
                "Source schema id must be non-negative.");
        this.scanSnapshotId = scanSnapshotId;
        this.sourceSchemaId = sourceSchemaId;
    }

    public long scanSnapshotId() {
        return scanSnapshotId;
    }

    @Nullable
    public Long sourceSchemaId() {
        return sourceSchemaId;
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(SNAPSHOT_ONLY_SIZE + Long.BYTES);
            output.writeInt(MAGIC);
            output.writeInt(sourceSchemaId == null ? VERSION : VERSION_WITH_SCHEMA);
            output.writeLong(scanSnapshotId);
            if (sourceSchemaId != null) {
                output.writeLong(sourceSchemaId);
            }
            return output.getCopyOfBuffer();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to serialize data-evolution index source metadata.", e);
        }
    }

    public static boolean isDataEvolutionMeta(@Nullable byte[] bytes) {
        if (bytes == null || bytes.length < Integer.BYTES) {
            return false;
        }
        try {
            return new DataInputDeserializer(bytes).readInt() == MAGIC;
        } catch (IOException e) {
            return false;
        }
    }

    public static DataEvolutionIndexSourceMeta deserialize(byte[] bytes) {
        try {
            DataInputDeserializer input = new DataInputDeserializer(bytes);
            int magic = input.readInt();
            checkArgument(magic == MAGIC, "Not data-evolution index source metadata.");
            int version = input.readInt();
            checkArgument(
                    version == VERSION || version == VERSION_WITH_SCHEMA,
                    "Unsupported data-evolution index source version: %s.",
                    version);
            long scanSnapshotId = input.readLong();
            Long sourceSchemaId = version == VERSION_WITH_SCHEMA ? input.readLong() : null;
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in data-evolution index source metadata.");
            return new DataEvolutionIndexSourceMeta(scanSnapshotId, sourceSchemaId);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to deserialize data-evolution index source metadata.", e);
        }
    }

    public static DataEvolutionIndexSourceMeta fromIndexFile(IndexFileMeta indexFile) {
        GlobalIndexMeta globalIndexMeta = indexFile.globalIndexMeta();
        checkArgument(
                globalIndexMeta != null && isDataEvolutionMeta(globalIndexMeta.sourceMeta()),
                "Index file %s has no data-evolution source metadata.",
                indexFile.fileName());
        return deserialize(globalIndexMeta.sourceMeta());
    }
}
