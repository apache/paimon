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

    private final Map<Identifier, Long> currentSnapshotIdMap;

    public CDCCheckpoint(
            Collection<TableAwareFileStoreSourceSplit> splits,
            Map<Identifier, Long> currentSnapshotIdMap) {
        this.splits = splits;
        this.currentSnapshotIdMap = currentSnapshotIdMap;
    }

    public Collection<TableAwareFileStoreSourceSplit> getSplits() {
        return splits;
    }

    public Map<Identifier, Long> getCurrentSnapshotIdMap() {
        return currentSnapshotIdMap;
    }

    @Override
    public int hashCode() {
        return Objects.hash(splits, currentSnapshotIdMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CDCCheckpoint)) {
            return false;
        }

        CDCCheckpoint cdcCheckpoint = (CDCCheckpoint) obj;
        return Objects.equals(cdcCheckpoint.splits, splits)
                && Objects.equals(cdcCheckpoint.currentSnapshotIdMap, currentSnapshotIdMap);
    }

    /** {@link SimpleVersionedSerializer} for {@link CDCCheckpoint}. */
    public static class Serializer implements SimpleVersionedSerializer<CDCCheckpoint> {
        private final SimpleVersionedSerializer<TableAwareFileStoreSourceSplit> splitSerializer =
                new TableAwareFileStoreSourceSplit.Serializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(CDCCheckpoint checkpoint) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

            view.writeInt(checkpoint.splits.size());
            for (TableAwareFileStoreSourceSplit split : checkpoint.splits) {
                byte[] bytes = splitSerializer.serialize(split);
                view.writeInt(bytes.length);
                view.write(bytes);
            }

            view.writeInt(checkpoint.currentSnapshotIdMap.size());
            for (Map.Entry<Identifier, Long> entry : checkpoint.currentSnapshotIdMap.entrySet()) {
                view.writeUTF(JsonSerdeUtil.toJson(entry.getKey()));
                view.writeLong(entry.getValue());
            }

            return out.toByteArray();
        }

        @Override
        public CDCCheckpoint deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer view = new DataInputDeserializer(serialized);

            int splitNumber = view.readInt();
            List<TableAwareFileStoreSourceSplit> splits = new ArrayList<>(splitNumber);
            for (int i = 0; i < splitNumber; i++) {
                int byteNumber = view.readInt();
                byte[] bytes = new byte[byteNumber];
                view.readFully(bytes);
                splits.add(splitSerializer.deserialize(version, bytes));
            }

            int currentSnapshotIdMapSize = view.readInt();
            Map<Identifier, Long> currentSnapshotIdMap = new HashMap<>(currentSnapshotIdMapSize);
            for (int i = 0; i < currentSnapshotIdMapSize; i++) {
                Identifier identifier = JsonSerdeUtil.fromJson(view.readUTF(), Identifier.class);
                long currentSnapshotId = view.readLong();
                currentSnapshotIdMap.put(identifier, currentSnapshotId);
            }

            return new CDCCheckpoint(splits, currentSnapshotIdMap);
        }
    }
}
