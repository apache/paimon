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

package org.apache.paimon.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A {@link SimpleVersionedSerializer} for {@link PendingSplitsCheckpoint}. */
public class PendingSplitsCheckpointSerializer
        implements SimpleVersionedSerializer<PendingSplitsCheckpoint> {

    private static final long INVALID_SNAPSHOT = -1;

    private final FileStoreSourceSplitSerializer splitSerializer;

    public PendingSplitsCheckpointSerializer(FileStoreSourceSplitSerializer splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(PendingSplitsCheckpoint pendingSplitsCheckpoint) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        view.writeInt(pendingSplitsCheckpoint.splits().size());
        for (FileStoreSourceSplit split : pendingSplitsCheckpoint.splits()) {
            byte[] bytes = splitSerializer.serialize(split);
            view.writeInt(bytes.length);
            view.write(bytes);
        }

        Long currentSnapshotId = pendingSplitsCheckpoint.currentSnapshotId();
        view.writeLong(currentSnapshotId == null ? INVALID_SNAPSHOT : currentSnapshotId);

        return out.toByteArray();
    }

    @Override
    public PendingSplitsCheckpoint deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);

        int splitNumber = view.readInt();
        List<FileStoreSourceSplit> splits = new ArrayList<>(splitNumber);
        for (int i = 0; i < splitNumber; i++) {
            int byteNumber = view.readInt();
            byte[] bytes = new byte[byteNumber];
            view.readFully(bytes);
            splits.add(splitSerializer.deserialize(version, bytes));
        }

        long currentSnapshotId = view.readLong();
        return new PendingSplitsCheckpoint(
                splits, currentSnapshotId == INVALID_SNAPSHOT ? null : currentSnapshotId);
    }
}
