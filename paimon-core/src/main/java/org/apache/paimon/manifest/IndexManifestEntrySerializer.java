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

package org.apache.paimon.manifest;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.index.IndexFileMetaSerializer.dvMetasToRowArrayData;
import static org.apache.paimon.index.IndexFileMetaSerializer.rowArrayDataToDvMetas;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** A {@link VersionedObjectSerializer} for {@link IndexManifestEntry}. */
public class IndexManifestEntrySerializer extends VersionedObjectSerializer<IndexManifestEntry> {

    public IndexManifestEntrySerializer() {
        super(IndexManifestEntry.SCHEMA);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public InternalRow convertTo(IndexManifestEntry record) {
        IndexFileMeta indexFile = record.indexFile();
        GlobalIndexMeta globalIndexMeta = indexFile.globalIndexMeta();
        InternalRow globalIndexRow =
                globalIndexMeta == null
                        ? null
                        : GenericRow.of(
                                globalIndexMeta.rowRangeStart(),
                                globalIndexMeta.rowRangeEnd(),
                                globalIndexMeta.indexFieldId(),
                                globalIndexMeta.indexMeta() == null
                                        ? null
                                        : new GenericArray(globalIndexMeta.extraFieldIds()),
                                globalIndexMeta.indexMeta());
        return GenericRow.of(
                record.kind().toByteValue(),
                serializeBinaryRow(record.partition()),
                record.bucket(),
                fromString(indexFile.indexType()),
                fromString(indexFile.fileName()),
                indexFile.fileSize(),
                indexFile.rowCount(),
                dvMetasToRowArrayData(indexFile.dvRanges()),
                fromString(indexFile.externalPath()),
                globalIndexRow);
    }

    @Override
    public IndexManifestEntry convertFrom(int version, InternalRow row) {
        if (version != 1) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        GlobalIndexMeta globalIndexMeta = null;
        if (!row.isNullAt(9)) {
            InternalRow globalIndexRow = row.getRow(9, 5);
            long rowRangeStart = globalIndexRow.getLong(0);
            long rowRangeEnd = globalIndexRow.getLong(1);
            int indexFieldId = globalIndexRow.getInt(2);
            int[] extralFields =
                    globalIndexRow.isNullAt(3) ? null : globalIndexRow.getArray(3).toIntArray();
            byte[] indexMeta = globalIndexRow.isNullAt(4) ? null : globalIndexRow.getBinary(4);
            globalIndexMeta =
                    new GlobalIndexMeta(
                            rowRangeStart, rowRangeEnd, indexFieldId, extralFields, indexMeta);
        }

        return new IndexManifestEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                new IndexFileMeta(
                        row.getString(3).toString(),
                        row.getString(4).toString(),
                        row.getLong(5),
                        row.getLong(6),
                        row.isNullAt(7) ? null : rowArrayDataToDvMetas(row.getArray(7)),
                        row.isNullAt(8) ? null : row.getString(8).toString(),
                        globalIndexMeta));
    }
}
