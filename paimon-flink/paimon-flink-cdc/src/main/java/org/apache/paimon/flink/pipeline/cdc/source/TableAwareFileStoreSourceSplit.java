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
import org.apache.paimon.schema.TableSchema;
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
    @Nullable private final TableSchema lastSchema;
    private final TableSchema schema;

    public TableAwareFileStoreSourceSplit(
            String id,
            Split split,
            long recordsToSkip,
            Identifier identifier,
            @Nullable TableSchema lastSchema,
            TableSchema schema) {
        super(id, split, recordsToSkip);
        this.identifier = identifier;
        this.lastSchema = lastSchema;
        this.schema = schema;
    }

    public Identifier getIdentifier() {
        return identifier;
    }

    public @Nullable TableSchema getLastSchema() {
        return lastSchema;
    }

    public TableSchema getSchema() {
        return schema;
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
                && Objects.equals(lastSchema, other.lastSchema)
                && schema.equals(other.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId(), split(), recordsToSkip(), identifier, lastSchema, schema);
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
                + ", lastSchema="
                + lastSchema
                + ", schema="
                + schema
                + '}';
    }

    /** The serializer for {@link TableAwareFileStoreSourceSplit}. */
    public static class Serializer
            implements SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> {

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
            view.writeUTF(JsonSerdeUtil.toJson(split.getLastSchema()));
            view.writeUTF(JsonSerdeUtil.toJson(split.getSchema()));
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
            TableSchema lastSchema = TableSchema.fromJson(view.readUTF());
            TableSchema schema = TableSchema.fromJson(view.readUTF());
            return new TableAwareFileStoreSourceSplit(
                    splitId, split, recordsToSkip, identifier, lastSchema, schema);
        }
    }
}
