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

package org.apache.paimon.table.source;

import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FallbackReadFileStoreTable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Versioned binary serializer for non-system table {@link Split}s.
 *
 * <p>The frame intentionally uses stable type ids rather than Java class names so that the bytes
 * can be consumed by non-Java runtimes. {@link org.apache.paimon.table.format.FormatDataSplit} and
 * object table splits are not part of this protocol.
 */
public class SplitSerializer {

    private static final long MAGIC = 0x53504C49545F5631L; // "SPLIT_V1"
    private static final int VERSION = 1;

    private static final int DATA_SPLIT = 1;
    private static final int INCREMENTAL_SPLIT = 2;
    private static final int INDEXED_SPLIT = 3;
    private static final int CHAIN_SPLIT = 4;
    private static final int QUERY_AUTH_SPLIT = 5;
    private static final int FALLBACK_DATA_SPLIT = 6;
    private static final int FALLBACK_SPLIT = 7;

    private SplitSerializer() {}

    public static byte[] serialize(Split split) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        serialize(split, new DataOutputViewStreamWrapper(out));
        return out.toByteArray();
    }

    public static void serialize(Split split, DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);

        if (split instanceof QueryAuthSplit) {
            out.writeInt(QUERY_AUTH_SPLIT);
            ((QueryAuthSplit) split).serialize(out);
        } else if (split instanceof FallbackReadFileStoreTable.FallbackDataSplit) {
            out.writeInt(FALLBACK_DATA_SPLIT);
            ((FallbackReadFileStoreTable.FallbackDataSplit) split).serialize(out);
        } else if (split instanceof FallbackReadFileStoreTable.FallbackSplitImpl) {
            out.writeInt(FALLBACK_SPLIT);
            ((FallbackReadFileStoreTable.FallbackSplitImpl) split).serialize(out);
        } else if (split instanceof IndexedSplit) {
            out.writeInt(INDEXED_SPLIT);
            ((IndexedSplit) split).serialize(out);
        } else if (split instanceof ChainSplit) {
            out.writeInt(CHAIN_SPLIT);
            ((ChainSplit) split).serialize(out);
        } else if (split instanceof IncrementalSplit) {
            out.writeInt(INCREMENTAL_SPLIT);
            ((IncrementalSplit) split).serialize(out);
        } else if (split instanceof DataSplit) {
            out.writeInt(DATA_SPLIT);
            ((DataSplit) split).serialize(out);
        } else {
            throw new IOException("Unsupported split class: " + split.getClass().getName());
        }
    }

    public static Split deserialize(byte[] bytes) throws IOException {
        return deserialize(new DataInputDeserializer(bytes));
    }

    public static Split deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        if (magic != MAGIC) {
            throw new IOException("Corrupted split: wrong magic number " + magic);
        }

        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported split serializer version: " + version);
        }

        int type = in.readInt();
        switch (type) {
            case DATA_SPLIT:
                return DataSplit.deserialize(in);
            case INCREMENTAL_SPLIT:
                return IncrementalSplit.deserialize(in);
            case INDEXED_SPLIT:
                return IndexedSplit.deserialize(in);
            case CHAIN_SPLIT:
                return ChainSplit.deserialize(in);
            case QUERY_AUTH_SPLIT:
                return QueryAuthSplit.deserialize(in);
            case FALLBACK_DATA_SPLIT:
                return FallbackReadFileStoreTable.FallbackDataSplit.deserialize(in);
            case FALLBACK_SPLIT:
                return FallbackReadFileStoreTable.FallbackSplitImpl.deserialize(in);
            default:
                throw new IOException("Unsupported split type: " + type);
        }
    }
}
