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

import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** A {@link SimpleVersionedSerializer} for {@link FileStoreSourceSplit}. */
public class FileStoreSourceSplitSerializer
        implements SimpleVersionedSerializer<FileStoreSourceSplit> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileStoreSourceSplit split) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(split.splitId());
        InstantiationUtil.serializeObject(view, split.split());
        view.writeLong(split.recordsToSkip());
        return out.toByteArray();
    }

    @Override
    public FileStoreSourceSplit deserialize(int version, byte[] serialized) throws IOException {
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
        return new FileStoreSourceSplit(splitId, split, recordsToSkip);
    }
}
