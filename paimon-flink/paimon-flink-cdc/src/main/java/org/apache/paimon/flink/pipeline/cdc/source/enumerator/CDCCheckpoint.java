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

package org.apache.paimon.flink.pipeline.cdc.source.enumerator;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.pipeline.cdc.source.TableAwareFileStoreSourceSplit;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A checkpoint of the current state of the containing the currently pending splits that are not yet
 * assigned, and the consumption progress of each table.
 */
public class CDCCheckpoint {
    private final Collection<TableAwareFileStoreSourceSplit> splits;

    private final Map<Identifier, TableProgress> tableProgressMap;

    public CDCCheckpoint(
            Collection<TableAwareFileStoreSourceSplit> splits,
            Map<Identifier, TableProgress> tableProgressMap) {
        this.splits = splits;
        this.tableProgressMap = tableProgressMap;
    }

    public Collection<TableAwareFileStoreSourceSplit> getSplits() {
        return splits;
    }

    public Map<Identifier, Long> getCurrentSnapshotIdMap() {
        Map<Identifier, Long> currentSnapshotIdMap = new HashMap<>();
        for (Map.Entry<Identifier, TableProgress> entry : tableProgressMap.entrySet()) {
            currentSnapshotIdMap.put(entry.getKey(), entry.getValue().nextSnapshotId());
        }
        return currentSnapshotIdMap;
    }

    public Map<Identifier, TableProgress> getTableProgressMap() {
        return tableProgressMap;
    }

    @Override
    public int hashCode() {
        return Objects.hash(splits, tableProgressMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CDCCheckpoint)) {
            return false;
        }

        CDCCheckpoint cdcCheckpoint = (CDCCheckpoint) obj;
        return Objects.equals(cdcCheckpoint.splits, splits)
                && Objects.equals(cdcCheckpoint.tableProgressMap, tableProgressMap);
    }

    /** Per-table progress stored in CDC enumerator checkpoints. */
    public static class TableProgress {
        @Nullable private final Long nextSnapshotId;
        @Nullable private final Long schemaId;

        public TableProgress(@Nullable Long nextSnapshotId, @Nullable Long schemaId) {
            this.nextSnapshotId = nextSnapshotId;
            this.schemaId = schemaId;
        }

        @Nullable
        public Long nextSnapshotId() {
            return nextSnapshotId;
        }

        @Nullable
        public Long schemaId() {
            return schemaId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nextSnapshotId, schemaId);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableProgress)) {
                return false;
            }

            TableProgress that = (TableProgress) obj;
            return Objects.equals(nextSnapshotId, that.nextSnapshotId)
                    && Objects.equals(schemaId, that.schemaId);
        }

        @Override
        public String toString() {
            return "TableProgress{"
                    + "nextSnapshotId="
                    + nextSnapshotId
                    + ", schemaId="
                    + schemaId
                    + '}';
        }
    }

    /** {@link SimpleVersionedSerializer} for {@link CDCCheckpoint}. */
    public static class Serializer implements SimpleVersionedSerializer<CDCCheckpoint> {
        private static final int VERSION_1 = 1;
        private static final int VERSION_2 = 2;

        private final SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> splitSerializer =
                new TableAwareFileStoreSourceSplit.Serializer();

        @Override
        public int getVersion() {
            return VERSION_2;
        }

        @Override
        public byte[] serialize(CDCCheckpoint checkpoint) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

            view.writeInt(checkpoint.splits.size());
            for (TableAwareFileStoreSourceSplit split : checkpoint.splits) {
                byte[] bytes = splitSerializer.serialize(split);
                view.writeInt(splitSerializer.getVersion());
                view.writeInt(bytes.length);
                view.write(bytes);
            }

            view.writeInt(checkpoint.tableProgressMap.size());
            for (Map.Entry<Identifier, TableProgress> entry :
                    checkpoint.tableProgressMap.entrySet()) {
                view.writeUTF(JsonSerdeUtil.toJson(entry.getKey()));
                writeNullableLong(view, entry.getValue().nextSnapshotId());
                writeNullableLong(view, entry.getValue().schemaId());
            }

            return out.toByteArray();
        }

        @Override
        public CDCCheckpoint deserialize(int version, byte[] serialized) throws IOException {
            if (version == VERSION_1) {
                return deserializeV1(serialized);
            }
            if (version != VERSION_2) {
                throw new IOException("Unsupported CDC checkpoint version: " + version);
            }

            DataInputDeserializer view = new DataInputDeserializer(serialized);

            int splitNumber = view.readInt();
            List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>(splitNumber);
            for (int i = 0; i < splitNumber; i++) {
                int splitVersion = view.readInt();
                int byteNumber = view.readInt();
                byte[] bytes = new byte[byteNumber];
                view.readFully(bytes);
                splits.add(splitSerializer.deserialize(splitVersion, bytes));
            }

            int tableProgressMapSize = view.readInt();
            Map<Identifier, TableProgress> tableProgressMap = new HashMap<>(tableProgressMapSize);
            for (int i = 0; i < tableProgressMapSize; i++) {
                Identifier identifier = JsonSerdeUtil.fromJson(view.readUTF(), Identifier.class);
                Long nextSnapshotId = readNullableLong(view);
                Long schemaId = readNullableLong(view);
                tableProgressMap.put(identifier, new TableProgress(nextSnapshotId, schemaId));
            }

            return new CDCCheckpoint(splits, tableProgressMap);
        }

        private CDCCheckpoint deserializeV1(byte[] serialized) throws IOException {
            DataInputDeserializer view = new DataInputDeserializer(serialized);

            int splitNumber = view.readInt();
            List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>(splitNumber);
            for (int i = 0; i < splitNumber; i++) {
                int byteNumber = view.readInt();
                byte[] bytes = new byte[byteNumber];
                view.readFully(bytes);
                splits.add(splitSerializer.deserialize(VERSION_1, bytes));
            }

            int currentSnapshotIdMapSize = view.readInt();
            Map<Identifier, TableProgress> tableProgressMap =
                    new HashMap<>(currentSnapshotIdMapSize);
            for (int i = 0; i < currentSnapshotIdMapSize; i++) {
                Identifier identifier = JsonSerdeUtil.fromJson(view.readUTF(), Identifier.class);
                long currentSnapshotId = view.readLong();
                tableProgressMap.put(identifier, new TableProgress(currentSnapshotId, null));
            }

            return new CDCCheckpoint(splits, tableProgressMap);
        }

        private void writeNullableLong(DataOutputViewStreamWrapper view, @Nullable Long value)
                throws IOException {
            view.writeBoolean(value != null);
            if (value != null) {
                view.writeLong(value);
            }
        }

        @Nullable
        private Long readNullableLong(DataInputDeserializer view) throws IOException {
            return view.readBoolean() ? view.readLong() : null;
        }
    }
}
