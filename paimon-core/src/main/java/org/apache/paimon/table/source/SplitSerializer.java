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

import org.apache.paimon.catalog.TableQueryAuthResult;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.utils.FunctionWithIOException;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

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
            writeQueryAuthSplit((QueryAuthSplit) split, out);
        } else if (split instanceof FallbackReadFileStoreTable.FallbackDataSplit) {
            out.writeInt(FALLBACK_DATA_SPLIT);
            ((FallbackReadFileStoreTable.FallbackDataSplit) split).serialize(out);
        } else if (split instanceof FallbackReadFileStoreTable.FallbackSplitImpl) {
            out.writeInt(FALLBACK_SPLIT);
            writeFallbackSplit((FallbackReadFileStoreTable.FallbackSplitImpl) split, out);
        } else if (split instanceof IndexedSplit) {
            out.writeInt(INDEXED_SPLIT);
            ((IndexedSplit) split).serialize(out);
        } else if (split instanceof ChainSplit) {
            out.writeInt(CHAIN_SPLIT);
            writeChainSplit((ChainSplit) split, out);
        } else if (split instanceof IncrementalSplit) {
            out.writeInt(INCREMENTAL_SPLIT);
            writeIncrementalSplit((IncrementalSplit) split, out);
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
                return readIncrementalSplit(in);
            case INDEXED_SPLIT:
                return IndexedSplit.deserialize(in);
            case CHAIN_SPLIT:
                return readChainSplit(in);
            case QUERY_AUTH_SPLIT:
                return readQueryAuthSplit(in);
            case FALLBACK_DATA_SPLIT:
                return FallbackReadFileStoreTable.FallbackDataSplit.deserialize(in);
            case FALLBACK_SPLIT:
                return readFallbackSplit(in);
            default:
                throw new IOException("Unsupported split type: " + type);
        }
    }

    private static void writeIncrementalSplit(IncrementalSplit split, DataOutputView out)
            throws IOException {
        out.writeLong(split.snapshotId());
        serializeBinaryRow(split.partition(), out);
        out.writeInt(split.bucket());
        out.writeInt(split.totalBuckets());
        writeDataFiles(split.beforeFiles(), out);
        DeletionFile.serializeList(out, split.beforeDeletionFiles());
        writeDataFiles(split.afterFiles(), out);
        DeletionFile.serializeList(out, split.afterDeletionFiles());
        out.writeBoolean(split.isStreaming());
    }

    private static IncrementalSplit readIncrementalSplit(DataInputView in) throws IOException {
        long snapshotId = in.readLong();
        BinaryRow partition = deserializeBinaryRow(in);
        int bucket = in.readInt();
        int totalBuckets = in.readInt();
        List<DataFileMeta> beforeFiles = readDataFiles(in);
        FunctionWithIOException<DataInputView, DeletionFile> deletionFileSerializer =
                DeletionFile::deserialize;
        List<DeletionFile> beforeDeletionFiles =
                DeletionFile.deserializeList(in, deletionFileSerializer);
        List<DataFileMeta> afterFiles = readDataFiles(in);
        List<DeletionFile> afterDeletionFiles =
                DeletionFile.deserializeList(in, deletionFileSerializer);
        boolean isStreaming = in.readBoolean();
        return new IncrementalSplit(
                snapshotId,
                partition,
                bucket,
                totalBuckets,
                beforeFiles,
                beforeDeletionFiles,
                afterFiles,
                afterDeletionFiles,
                isStreaming);
    }

    private static void writeChainSplit(ChainSplit split, DataOutputView out) throws IOException {
        serializeBinaryRow(split.logicalPartition(), out);
        writeDataFiles(split.dataFiles(), out);
        writeStringMap(out, split.fileBucketPathMapping());
        writeStringMap(out, split.fileBranchMapping());
    }

    private static ChainSplit readChainSplit(DataInputView in) throws IOException {
        BinaryRow logicalPartition = deserializeBinaryRow(in);
        List<DataFileMeta> dataFiles = readDataFiles(in);
        Map<String, String> fileBucketPathMapping = readStringMap(in);
        Map<String, String> fileBranchMapping = readStringMap(in);
        return new ChainSplit(
                logicalPartition, dataFiles, fileBranchMapping, fileBucketPathMapping);
    }

    private static void writeQueryAuthSplit(QueryAuthSplit split, DataOutputView out)
            throws IOException {
        serialize(split.split(), out);
        writeAuthResult(out, split.authResult());
    }

    private static QueryAuthSplit readQueryAuthSplit(DataInputView in) throws IOException {
        Split split = deserialize(in);
        TableQueryAuthResult authResult = readAuthResult(in);
        return new QueryAuthSplit(split, authResult);
    }

    private static void writeFallbackSplit(
            FallbackReadFileStoreTable.FallbackSplitImpl split, DataOutputView out)
            throws IOException {
        out.writeBoolean(split.isFallback());
        serialize(split.wrapped(), out);
    }

    private static FallbackReadFileStoreTable.FallbackSplitImpl readFallbackSplit(DataInputView in)
            throws IOException {
        boolean isFallback = in.readBoolean();
        Split split = deserialize(in);
        return new FallbackReadFileStoreTable.FallbackSplitImpl(split, isFallback);
    }

    private static void writeAuthResult(
            DataOutputView out, @Nullable TableQueryAuthResult authResult) throws IOException {
        if (authResult == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        writeStringList(out, authResult.filter());
        writeStringMap(out, authResult.columnMasking());
    }

    @Nullable
    private static TableQueryAuthResult readAuthResult(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return new TableQueryAuthResult(readStringList(in), readNullableStringMap(in));
    }

    private static void writeDataFiles(List<DataFileMeta> files, DataOutputView out)
            throws IOException {
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        out.writeInt(files.size());
        for (DataFileMeta file : files) {
            serializer.serialize(file, out);
        }
    }

    private static List<DataFileMeta> readDataFiles(DataInputView in) throws IOException {
        int size = in.readInt();
        List<DataFileMeta> files = new ArrayList<>(size);
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        for (int i = 0; i < size; i++) {
            files.add(serializer.deserialize(in));
        }
        return files;
    }

    private static void writeStringList(DataOutputView out, @Nullable List<String> strings)
            throws IOException {
        if (strings == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(strings.size());
        for (String string : strings) {
            writeString(out, string);
        }
    }

    @Nullable
    private static List<String> readStringList(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int size = in.readInt();
        List<String> strings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            strings.add(readString(in));
        }
        return strings;
    }

    private static void writeStringMap(DataOutputView out, @Nullable Map<String, String> map)
            throws IOException {
        if (map == null) {
            out.writeBoolean(false);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(map.size());
        for (Map.Entry<String, String> entry : new TreeMap<>(map).entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    private static Map<String, String> readStringMap(DataInputView in) throws IOException {
        Map<String, String> map = readNullableStringMap(in);
        return map == null ? new HashMap<>() : map;
    }

    @Nullable
    private static Map<String, String> readNullableStringMap(DataInputView in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }

        int size = in.readInt();
        Map<String, String> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            map.put(readString(in), readString(in));
        }
        return map;
    }

    private static void writeString(DataOutputView out, @Nullable String string)
            throws IOException {
        if (string == null) {
            out.writeInt(-1);
            return;
        }

        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Nullable
    private static String readString(DataInputView in) throws IOException {
        int length = in.readInt();
        if (length < 0) {
            return null;
        }

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
