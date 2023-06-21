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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** {@link SimpleVersionedSerializer} for {@link AlignedSourceSplit}. */
public class AlignedSourceSplitSerializer implements SimpleVersionedSerializer<AlignedSourceSplit> {

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(AlignedSourceSplit alignedSourceSplit) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos)) {
            List<Split> splits = alignedSourceSplit.getSplits();
            view.writeInt(splits.size());
            for (Split split : splits) {
                InstantiationUtil.serializeObject(view, split);
            }
            view.writeLong(alignedSourceSplit.getNextSnapshotId());
            view.writeBoolean(alignedSourceSplit.isPlaceHolder());
            return baos.toByteArray();
        }
    }

    @Override
    public AlignedSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream in = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in)) {
            int size = view.readInt();
            List<Split> splits = new ArrayList<>(size);
            ClassLoader classLoader = getClass().getClassLoader();
            Split split;
            try {
                for (int i = 0; i < size; i++) {
                    split = InstantiationUtil.deserializeObject(in, classLoader);
                    splits.add(split);
                }
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
            long nextSnapshotId = view.readLong();
            boolean isPlaceHolder = view.readBoolean();
            return new AlignedSourceSplit(splits, nextSnapshotId, isPlaceHolder);
        }
    }
}
