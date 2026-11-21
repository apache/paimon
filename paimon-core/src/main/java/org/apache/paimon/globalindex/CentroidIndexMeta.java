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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Core-owned indexMeta envelope. Ordinary index files keep their original opaque metadata. */
public final class CentroidIndexMeta {
    private static final byte[] MAGIC = {'P', 'A', 'I', 'M', 'C', 'E', 'N', 'T'};
    private static final byte VERSION = 1;
    private static final byte SCALAR_INDEX_VERSION = 2;
    private static final byte ROUTER = 1;
    private static final byte SHARD = 2;
    private static final byte SCALAR_INDEX = 3;
    private static final int HEADER_SIZE = MAGIC.length + 2 + 3 * Integer.BYTES;

    private CentroidIndexMeta() {}

    public static boolean isWrapped(@Nullable byte[] meta) {
        if (meta == null || meta.length < MAGIC.length) {
            return false;
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (meta[i] != MAGIC[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isRoutingModel(IndexFileMeta file) {
        return file.globalIndexMeta() != null && isRoutingModel(file.globalIndexMeta().indexMeta());
    }

    public static boolean isRoutingModel(@Nullable byte[] meta) {
        return isWrapped(meta) && role(meta) == ROUTER;
    }

    public static boolean hasScalarIndexes(@Nullable byte[] meta) {
        return isWrapped(meta)
                && meta.length >= HEADER_SIZE
                && meta[MAGIC.length] == SCALAR_INDEX_VERSION;
    }

    public static boolean isScalarIndex(IndexFileMeta file) {
        byte[] meta = checkNotNull(file.globalIndexMeta()).indexMeta();
        return isWrapped(meta) && role(meta) == SCALAR_INDEX;
    }

    /** Scalar sections belong to the primary vector index for create and drop operations. */
    public static boolean matchesIndexFields(GlobalIndexMeta meta, List<Integer> fieldIds) {
        return meta.getIndexedFieldIds().equals(fieldIds)
                || (fieldIds.size() == 1
                        && meta.indexFieldId() == fieldIds.get(0)
                        && hasScalarIndexes(meta.indexMeta()));
    }

    private static byte role(byte[] meta) {
        checkArgument(meta.length >= HEADER_SIZE, "Truncated centroid metadata.");
        byte version = meta[MAGIC.length];
        checkArgument(
                version == VERSION || version == SCALAR_INDEX_VERSION,
                "Unsupported centroid metadata version: %s.",
                version);
        byte role = meta[MAGIC.length + 1];
        checkArgument(
                role == ROUTER
                        || role == SHARD
                        || (version == SCALAR_INDEX_VERSION && role == SCALAR_INDEX),
                "Invalid centroid file role.");
        return role;
    }

    public static ResultEntry router(ResultEntry file, String modelId, int nlist) {
        return router(file, modelId, nlist, false);
    }

    public static ResultEntry router(ResultEntry file, String modelId, int nlist, boolean bitmap) {
        return wrap(file, modelId, bitmap ? SCALAR_INDEX_VERSION : VERSION, ROUTER, nlist);
    }

    public static ResultEntry shard(ResultEntry file, String modelId, int centroid) {
        return shard(file, modelId, centroid, false);
    }

    public static ResultEntry shard(
            ResultEntry file, String modelId, int centroid, boolean bitmap) {
        return wrap(file, modelId, bitmap ? SCALAR_INDEX_VERSION : VERSION, SHARD, centroid);
    }

    public static ResultEntry scalarIndex(ResultEntry file, String modelId, int centroid) {
        return wrap(file, modelId, SCALAR_INDEX_VERSION, SCALAR_INDEX, centroid);
    }

    private static ResultEntry wrap(
            ResultEntry file, String modelId, byte version, byte role, int countOrId) {
        checkArgument(!modelId.isEmpty() && countOrId >= 0, "Invalid centroid descriptor.");
        byte[] id = modelId.getBytes(StandardCharsets.UTF_8);
        byte[] payload = file.meta();
        ByteBuffer out =
                ByteBuffer.allocate(
                        Math.addExact(
                                Math.addExact(HEADER_SIZE, id.length),
                                payload == null ? 0 : payload.length));
        out.put(MAGIC)
                .put(version)
                .put(role)
                .putInt(countOrId)
                .putInt(id.length)
                .putInt(payload == null ? -1 : payload.length)
                .put(id);
        if (payload != null) {
            out.put(payload);
        }
        return new ResultEntry(file.filePath(), file.rowCount(), out.array());
    }

    public static GlobalIndexIOMeta file(IndexFileMeta file, IndexPathFactory paths) {
        return file(
                paths.toPath(file),
                file.fileSize(),
                file.rowCount(),
                checkNotNull(file.globalIndexMeta()).indexMeta());
    }

    public static GlobalIndexIOMeta file(Path path, long size, long rows, @Nullable byte[] meta) {
        if (!isWrapped(meta)) {
            return new GlobalIndexIOMeta(path, size, rows, meta);
        }
        byte role = role(meta);
        ByteBuffer in = ByteBuffer.wrap(meta);
        in.position(MAGIC.length + 2);
        int countOrId = in.getInt();
        int idLength = in.getInt();
        int payloadLength = in.getInt();
        checkArgument(
                countOrId >= 0
                        && idLength > 0
                        && idLength <= in.remaining()
                        && payloadLength >= -1
                        && in.remaining() - idLength == Math.max(0, payloadLength),
                "Invalid centroid metadata lengths.");
        String id = new String(meta, in.position(), idLength, StandardCharsets.UTF_8);
        in.position(in.position() + idLength);
        byte[] payload = null;
        if (payloadLength >= 0) {
            payload = new byte[payloadLength];
            in.get(payload);
        }
        return new CentroidIndexFile(
                path, size, rows, payload, id, role == ROUTER, role == SCALAR_INDEX, countOrId);
    }
}
