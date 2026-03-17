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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactTask;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.utils.Range;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * {@link SimpleVersionedSerializer} for {@link BTreeGlobalIndexCompactTask}.
 *
 * <p>Uses manual binary serialization because {@link IndexManifestEntry} and related classes are
 * not Java-serializable.
 */
public class BTreeGlobalIndexCompactTaskSimpleSerializer
        implements SimpleVersionedSerializer<BTreeGlobalIndexCompactTask> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BTreeGlobalIndexCompactTask task) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);

        serializeBinaryRow(task.partition(), out);
        out.writeInt(task.bucket());
        out.writeUTF(task.indexType());
        out.writeInt(task.indexFieldId());

        // merged range
        out.writeLong(task.mergedRange().from);
        out.writeLong(task.mergedRange().to);

        // range groups
        List<BTreeGlobalIndexCompactTask.RangeGroup> groups = task.rangeGroups();
        out.writeInt(groups.size());
        for (BTreeGlobalIndexCompactTask.RangeGroup group : groups) {
            out.writeLong(group.range().from);
            out.writeLong(group.range().to);
            serializeEntryList(group.files(), out);
        }

        return baos.toByteArray();
    }

    @Override
    public BTreeGlobalIndexCompactTask deserialize(int version, byte[] bytes) throws IOException {
        if (version != getVersion()) {
            throw new IOException(
                    "Cannot deserialize version: " + version + ", expected: " + getVersion());
        }
        DataInputDeserializer in = new DataInputDeserializer(bytes);

        BinaryRow partition = deserializeBinaryRow(in);
        int bucket = in.readInt();
        String indexType = in.readUTF();
        int indexFieldId = in.readInt();

        long mergedFrom = in.readLong();
        long mergedTo = in.readLong();
        Range mergedRange = new Range(mergedFrom, mergedTo);

        int groupCount = in.readInt();
        List<BTreeGlobalIndexCompactTask.RangeGroup> groups = new ArrayList<>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            long rangeFrom = in.readLong();
            long rangeTo = in.readLong();
            Range range = new Range(rangeFrom, rangeTo);
            List<IndexManifestEntry> files = deserializeEntryList(in);
            groups.add(new BTreeGlobalIndexCompactTask.RangeGroup(range, files));
        }

        return new BTreeGlobalIndexCompactTask(
                partition, bucket, indexType, indexFieldId, groups, mergedRange);
    }

    private void serializeEntryList(List<IndexManifestEntry> entries, DataOutputView out)
            throws IOException {
        out.writeInt(entries.size());
        for (IndexManifestEntry entry : entries) {
            serializeEntry(entry, out);
        }
    }

    private List<IndexManifestEntry> deserializeEntryList(DataInputDeserializer in)
            throws IOException {
        int size = in.readInt();
        List<IndexManifestEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            entries.add(deserializeEntry(in));
        }
        return entries;
    }

    private void serializeEntry(IndexManifestEntry entry, DataOutputView out) throws IOException {
        out.writeByte(entry.kind().toByteValue());
        serializeBinaryRow(entry.partition(), out);
        out.writeInt(entry.bucket());
        serializeIndexFileMeta(entry.indexFile(), out);
    }

    private IndexManifestEntry deserializeEntry(DataInputDeserializer in) throws IOException {
        FileKind kind = FileKind.fromByteValue(in.readByte());
        BinaryRow partition = deserializeBinaryRow(in);
        int bucket = in.readInt();
        IndexFileMeta indexFile = deserializeIndexFileMeta(in);
        return new IndexManifestEntry(kind, partition, bucket, indexFile);
    }

    private void serializeIndexFileMeta(IndexFileMeta meta, DataOutputView out) throws IOException {
        out.writeUTF(meta.indexType());
        out.writeUTF(meta.fileName());
        out.writeLong(meta.fileSize());
        out.writeLong(meta.rowCount());

        // externalPath (nullable)
        if (meta.externalPath() != null) {
            out.writeBoolean(true);
            out.writeUTF(meta.externalPath());
        } else {
            out.writeBoolean(false);
        }

        // globalIndexMeta (nullable)
        if (meta.globalIndexMeta() != null) {
            out.writeBoolean(true);
            serializeGlobalIndexMeta(meta.globalIndexMeta(), out);
        } else {
            out.writeBoolean(false);
        }
    }

    private IndexFileMeta deserializeIndexFileMeta(DataInputDeserializer in) throws IOException {
        String indexType = in.readUTF();
        String fileName = in.readUTF();
        long fileSize = in.readLong();
        long rowCount = in.readLong();

        String externalPath = null;
        if (in.readBoolean()) {
            externalPath = in.readUTF();
        }

        GlobalIndexMeta globalIndexMeta = null;
        if (in.readBoolean()) {
            globalIndexMeta = deserializeGlobalIndexMeta(in);
        }

        return new IndexFileMeta(
                indexType, fileName, fileSize, rowCount, globalIndexMeta, externalPath);
    }

    private void serializeGlobalIndexMeta(GlobalIndexMeta meta, DataOutputView out)
            throws IOException {
        out.writeLong(meta.rowRangeStart());
        out.writeLong(meta.rowRangeEnd());
        out.writeInt(meta.indexFieldId());

        // extraFieldIds (nullable)
        if (meta.extraFieldIds() != null) {
            out.writeBoolean(true);
            out.writeInt(meta.extraFieldIds().length);
            for (int id : meta.extraFieldIds()) {
                out.writeInt(id);
            }
        } else {
            out.writeBoolean(false);
        }

        // indexMeta bytes (nullable)
        if (meta.indexMeta() != null) {
            out.writeBoolean(true);
            out.writeInt(meta.indexMeta().length);
            out.write(meta.indexMeta());
        } else {
            out.writeBoolean(false);
        }
    }

    private GlobalIndexMeta deserializeGlobalIndexMeta(DataInputDeserializer in)
            throws IOException {
        long rowRangeStart = in.readLong();
        long rowRangeEnd = in.readLong();
        int indexFieldId = in.readInt();

        int[] extraFieldIds = null;
        if (in.readBoolean()) {
            int len = in.readInt();
            extraFieldIds = new int[len];
            for (int i = 0; i < len; i++) {
                extraFieldIds[i] = in.readInt();
            }
        }

        byte[] indexMeta = null;
        if (in.readBoolean()) {
            int len = in.readInt();
            indexMeta = new byte[len];
            in.readFully(indexMeta);
        }

        return new GlobalIndexMeta(
                rowRangeStart, rowRangeEnd, indexFieldId, extraFieldIds, indexMeta);
    }
}
