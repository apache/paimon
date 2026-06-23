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

package org.apache.paimon.flink.pipeline.cdc.source;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * A {@link FileStoreSourceSplit} that contains information about the table that corresponds to the
 * {@link Split}.
 */
public class TableAwareFileStoreSourceSplit extends FileStoreSourceSplit {
    private final Identifier identifier;
    private final @Nullable Long lastSchemaId;
    private final long schemaId;
    private final long schemaChangeEventsToSkip;
    private final boolean legacySchemaProgress;

    public TableAwareFileStoreSourceSplit(
            String id,
            Split split,
            long recordsToSkip,
            Identifier identifier,
            @Nullable Long lastSchemaId,
            long schemaId) {
        this(id, split, recordsToSkip, identifier, lastSchemaId, schemaId, 0L);
    }

    public TableAwareFileStoreSourceSplit(
            String id,
            Split split,
            long recordsToSkip,
            Identifier identifier,
            @Nullable Long lastSchemaId,
            long schemaId,
            long schemaChangeEventsToSkip) {
        this(
                id,
                split,
                recordsToSkip,
                identifier,
                lastSchemaId,
                schemaId,
                schemaChangeEventsToSkip,
                false);
    }

    private TableAwareFileStoreSourceSplit(
            String id,
            Split split,
            long recordsToSkip,
            Identifier identifier,
            @Nullable Long lastSchemaId,
            long schemaId,
            long schemaChangeEventsToSkip,
            boolean legacySchemaProgress) {
        super(id, split, recordsToSkip);
        this.identifier = identifier;
        this.lastSchemaId = lastSchemaId;
        this.schemaId = schemaId;
        this.schemaChangeEventsToSkip = schemaChangeEventsToSkip;
        this.legacySchemaProgress = legacySchemaProgress;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public @Nullable Long getLastSchemaId() {
        return lastSchemaId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public long schemaChangeEventsToSkip() {
        return schemaChangeEventsToSkip;
    }

    public TableAwareFileStoreSourceSplit updateWithProgress(
            long recordsToSkip, long schemaChangeEventsToSkip) {
        return new TableAwareFileStoreSourceSplit(
                splitId(),
                split(),
                recordsToSkip,
                identifier,
                lastSchemaId,
                schemaId,
                schemaChangeEventsToSkip,
                legacySchemaProgress);
    }

    public boolean isLegacySchemaProgress() {
        return legacySchemaProgress;
    }

    @Override
    public TableAwareFileStoreSourceSplit updateWithRecordsToSkip(long recordsToSkip) {
        return new TableAwareFileStoreSourceSplit(
                splitId(),
                split(),
                recordsToSkip,
                identifier,
                lastSchemaId,
                schemaId,
                schemaChangeEventsToSkip,
                legacySchemaProgress);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableAwareFileStoreSourceSplit)) {
            return false;
        }

        TableAwareFileStoreSourceSplit other = (TableAwareFileStoreSourceSplit) o;
        return Objects.equals(splitId(), other.splitId())
                && Objects.equals(split(), other.split())
                && recordsToSkip() == other.recordsToSkip()
                && identifier.equals(other.identifier)
                && Objects.equals(lastSchemaId, other.lastSchemaId)
                && schemaId == other.schemaId
                && schemaChangeEventsToSkip == other.schemaChangeEventsToSkip
                && legacySchemaProgress == other.legacySchemaProgress;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                splitId(),
                split(),
                recordsToSkip(),
                identifier,
                lastSchemaId,
                schemaId,
                schemaChangeEventsToSkip,
                legacySchemaProgress);
    }

    @Override
    public String toString() {
        return "TableAwareFileStoreSourceSplit{"
                + "splitId='"
                + splitId()
                + "'"
                + ", split="
                + split()
                + ", recordsToSkip="
                + recordsToSkip()
                + ", identifier="
                + identifier
                + ", lastSchemaId="
                + lastSchemaId
                + ", schemaId="
                + schemaId
                + ", schemaChangeEventsToSkip="
                + schemaChangeEventsToSkip
                + ", legacySchemaProgress="
                + legacySchemaProgress
                + '}';
    }

    /** The serializer for {@link TableAwareFileStoreSourceSplit}. */
    public static class Serializer
            implements SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> {
        private static final int VERSION_1 = 1;
        private static final int VERSION_2 = 2;
        private static final Long NULL_SCHEMA_ID = -1L;

        @Override
        public int getVersion() {
            return VERSION_2;
        }

        @Override
        public byte[] serialize(TableAwareFileStoreSourceSplit split) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
            view.writeUTF(split.splitId());
            InstantiationUtil.serializeObject(view, split.split());
            view.writeLong(split.recordsToSkip());
            view.writeUTF(JsonSerdeUtil.toJson(split.getIdentifier()));
            view.writeLong(
                    split.getLastSchemaId() == null ? NULL_SCHEMA_ID : split.getLastSchemaId());
            view.writeLong(split.getSchemaId());
            view.writeLong(split.schemaChangeEventsToSkip());
            view.writeBoolean(split.isLegacySchemaProgress());
            return out.toByteArray();
        }

        @Override
        public TableAwareFileStoreSourceSplit deserialize(int version, byte[] serialized)
                throws IOException {
            if (version != VERSION_1 && version != VERSION_2) {
                throw new IOException(
                        "Unsupported TableAwareFileStoreSourceSplit version: " + version);
            }
            ByteArrayInputStream in = new ByteArrayInputStream(serialized);
            DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
            String splitId = view.readUTF();
            Split split;
            try {
                split = InstantiationUtil.deserializeObject(in, getClass().getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
            long recordsToSkip = view.readLong();
            Identifier identifier = JsonSerdeUtil.fromJson(view.readUTF(), Identifier.class);
            Long lastSchemaId = view.readLong();
            if (lastSchemaId.equals(NULL_SCHEMA_ID)) {
                lastSchemaId = null;
            }
            long schemaId = view.readLong();
            long schemaChangeEventsToSkip = version == VERSION_2 ? view.readLong() : 0L;
            boolean legacySchemaProgress =
                    version == VERSION_2 ? view.readBoolean() : version == VERSION_1;
            return new TableAwareFileStoreSourceSplit(
                    splitId,
                    split,
                    recordsToSkip,
                    identifier,
                    lastSchemaId,
                    schemaId,
                    schemaChangeEventsToSkip,
                    legacySchemaProgress);
        }
    }
}
