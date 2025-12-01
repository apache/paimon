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

    public TableAwareFileStoreSourceSplit(
            String id,
            Split split,
            long recordsToSkip,
            Identifier identifier,
            @Nullable Long lastSchemaId,
            long schemaId) {
        super(id, split, recordsToSkip);
        this.identifier = identifier;
        this.lastSchemaId = lastSchemaId;
        this.schemaId = schemaId;
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
                && schemaId == other.schemaId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                splitId(), split(), recordsToSkip(), identifier, lastSchemaId, schemaId);
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
                + '}';
    }

    /** The serializer for {@link TableAwareFileStoreSourceSplit}. */
    public static class Serializer
            implements SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> {
        private static final Long NULL_SCHEMA_ID = -1L;

        @Override
        public int getVersion() {
            return 1;
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
            return out.toByteArray();
        }

        @Override
        public TableAwareFileStoreSourceSplit deserialize(int version, byte[] serialized)
                throws IOException {
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
            return new TableAwareFileStoreSourceSplit(
                    splitId, split, recordsToSkip, identifier, lastSchemaId, schemaId);
        }
    }
}
