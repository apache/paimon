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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link IcebergManifestEntry}. */
public class IcebergManifestEntrySerializer extends ObjectSerializer<IcebergManifestEntry> {

    private static final long serialVersionUID = 1L;

    private final IcebergDataFileMetaSerializer fileSerializer;

    public IcebergManifestEntrySerializer(RowType partitionType) {
        super(IcebergManifestEntry.schema(partitionType));
        this.fileSerializer = new IcebergDataFileMetaSerializer(partitionType);
    }

    @Override
    public InternalRow toRow(IcebergManifestEntry entry) {
        return GenericRow.of(
                entry.status().id(),
                entry.snapshotId(),
                entry.sequenceNumber(),
                entry.fileSequenceNumber(),
                fileSerializer.toRow(entry.file()));
    }

    @Override
    public IcebergManifestEntry fromRow(InternalRow row) {
        return new IcebergManifestEntry(
                IcebergManifestEntry.Status.fromId(row.getInt(0)),
                row.getLong(1),
                row.getLong(2),
                row.getLong(3),
                fileSerializer.fromRow(row.getRow(4, fileSerializer.numFields())));
    }
}
