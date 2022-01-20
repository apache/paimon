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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.store.file.mergetree.Increment;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMetaSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link SimpleVersionedSerializer} for {@link LocalCommittable}. */
public class LocalCommittableSerializer implements SimpleVersionedSerializer<LocalCommittable> {

    private final BinaryRowDataSerializer partSerializer;
    private final SstFileMetaSerializer sstSerializer;

    public LocalCommittableSerializer(RowType partitionType, RowType keyType, RowType rowType) {
        this.partSerializer = new BinaryRowDataSerializer(partitionType.getFieldCount());
        this.sstSerializer = new SstFileMetaSerializer(keyType, rowType);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(LocalCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        partSerializer.serialize(obj.partition(), view);
        view.writeInt(obj.bucket());
        serializeFiles(view, obj.increment().newFiles());
        serializeFiles(view, obj.increment().compactBefore());
        serializeFiles(view, obj.increment().compactAfter());
        return out.toByteArray();
    }

    private void serializeFiles(DataOutputViewStreamWrapper view, List<SstFileMeta> files)
            throws IOException {
        view.writeInt(files.size());
        for (SstFileMeta file : files) {
            sstSerializer.serialize(file, view);
        }
    }

    private List<SstFileMeta> deserializeFiles(DataInputDeserializer view) throws IOException {
        int fileNumber = view.readInt();
        List<SstFileMeta> files = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            files.add(sstSerializer.deserialize(view));
        }
        return files;
    }

    @Override
    public LocalCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return new LocalCommittable(
                partSerializer.deserialize(view),
                view.readInt(),
                new Increment(
                        deserializeFiles(view), deserializeFiles(view), deserializeFiles(view)));
    }
}
