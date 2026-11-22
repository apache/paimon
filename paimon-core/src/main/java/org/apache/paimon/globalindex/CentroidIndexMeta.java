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
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Core-owned indexMeta envelope. Ordinary index files keep their original opaque metadata. */
public final class CentroidIndexMeta {
    private static final byte[] MAGIC = {'P', 'A', 'I', 'M', 'C', 'E', 'N', 'T'};
    private static final byte VERSION = 1;
    private static final byte ROUTER = 'R';
    private static final byte SHARD = 'S';
    private static final byte BITMAP = 'B';
    private static final int HEADER_SIZE = MAGIC.length + 2 + Integer.BYTES;

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

    /** Whether the index definition includes Bitmap fields, even in an all-null range. */
    public static boolean hasBitmapFields(GlobalIndexMeta meta) {
        int[] fields = meta.extraFieldIds();
        return fields != null && fields.length > 0 && isWrapped(meta.indexMeta());
    }

    public static boolean isScalarIndex(IndexFileMeta file) {
        byte[] meta = checkNotNull(file.globalIndexMeta()).indexMeta();
        return isWrapped(meta) && role(meta) == BITMAP;
    }

    /** Scalar sections belong to the primary vector index for create and drop operations. */
    public static boolean matchesIndexFields(GlobalIndexMeta meta, List<Integer> fieldIds) {
        return meta.getIndexedFieldIds().equals(fieldIds)
                || (fieldIds.size() == 1
                        && meta.indexFieldId() == fieldIds.get(0)
                        && hasBitmapFields(meta));
    }

    private static byte role(byte[] meta) {
        checkArgument(meta.length >= HEADER_SIZE, "Truncated centroid metadata.");
        byte version = meta[MAGIC.length + 1];
        checkArgument(version == VERSION, "Unsupported centroid metadata version: %s.", version);
        byte role = meta[MAGIC.length];
        checkArgument(
                role == ROUTER || role == SHARD || role == BITMAP, "Invalid centroid file role.");
        return role;
    }

    public static ResultEntry router(ResultEntry file, int nlist) {
        return wrap(file, ROUTER, nlist);
    }

    public static ResultEntry shard(ResultEntry file, int centroid) {
        return wrap(file, SHARD, centroid);
    }

    public static ResultEntry scalarIndex(ResultEntry file, int centroid) {
        return wrap(file, BITMAP, centroid);
    }

    private static ResultEntry wrap(ResultEntry file, byte role, int countOrId) {
        checkArgument(countOrId >= 0, "Invalid centroid descriptor.");
        byte[] payload = file.meta();
        ByteBuffer out =
                ByteBuffer.allocate(
                        Math.addExact(HEADER_SIZE, payload == null ? 0 : payload.length));
        out.put(MAGIC).put(role).put(VERSION).putInt(countOrId);
        if (payload != null) {
            out.put(payload);
        }
        return new ResultEntry(file.fileName(), file.rowCount(), out.array());
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
        checkArgument(countOrId >= 0, "Invalid centroid descriptor.");
        byte[] payload = null;
        if (in.hasRemaining()) {
            payload = new byte[in.remaining()];
            in.get(payload);
        }
        return new CentroidIndexFile(
                path, size, rows, payload, role == ROUTER, role == BITMAP, countOrId);
    }
}
