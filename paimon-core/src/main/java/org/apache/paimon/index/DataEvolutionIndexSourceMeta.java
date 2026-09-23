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

/** Data snapshot scanned to build a global index for a data-evolution table. */
public final class DataEvolutionIndexSourceMeta {

    // "DEIX". The marker distinguishes this metadata from primary-key index source metadata.
    private static final int MAGIC = 0x44454958;
    private static final int VERSION = 1;

    private final long scanSnapshotId;

    public DataEvolutionIndexSourceMeta(long scanSnapshotId) {
        checkArgument(scanSnapshotId > 0, "Scan snapshot id must be positive.");
        this.scanSnapshotId = scanSnapshotId;
    }

    public long scanSnapshotId() {
        return scanSnapshotId;
    }

    public byte[] serialize() {
        try {
            DataOutputSerializer output = new DataOutputSerializer(16);
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
            output.writeLong(scanSnapshotId);
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
                    version == VERSION,
                    "Unsupported data-evolution index source version: %s.",
                    version);
            long scanSnapshotId = input.readLong();
            checkArgument(
                    input.available() == 0,
                    "Unexpected trailing bytes in data-evolution index source metadata.");
            return new DataEvolutionIndexSourceMeta(scanSnapshotId);
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
