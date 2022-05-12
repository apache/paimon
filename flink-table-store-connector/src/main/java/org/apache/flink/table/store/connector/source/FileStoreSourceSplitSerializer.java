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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.store.file.data.DataFileMetaSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.apache.flink.table.store.file.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.flink.table.store.file.utils.SerializationUtils.serializeBinaryRow;

/** A {@link SimpleVersionedSerializer} for {@link FileStoreSourceSplit}. */
public class FileStoreSourceSplitSerializer
        implements SimpleVersionedSerializer<FileStoreSourceSplit> {

    private final DataFileMetaSerializer dataFileSerializer;

    public FileStoreSourceSplitSerializer() {
        this.dataFileSerializer = new DataFileMetaSerializer();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileStoreSourceSplit split) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(split.splitId());
        serializeBinaryRow(split.partition(), view);
        view.writeInt(split.bucket());
        dataFileSerializer.serializeList(split.files(), view);
        view.writeLong(split.recordsToSkip());
        return out.toByteArray();
    }

    @Override
    public FileStoreSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer view = new DataInputDeserializer(serialized);
        return new FileStoreSourceSplit(
                view.readUTF(),
                deserializeBinaryRow(view),
                view.readInt(),
                dataFileSerializer.deserializeList(view),
                view.readLong());
    }
}
