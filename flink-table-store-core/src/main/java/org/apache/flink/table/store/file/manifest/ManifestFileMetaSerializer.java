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

package org.apache.flink.table.store.file.manifest;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.VersionedObjectSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Serializer for {@link ManifestFileMeta}. */
public class ManifestFileMetaSerializer extends VersionedObjectSerializer<ManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    private final FieldStatsArraySerializer statsArraySerializer;

    public ManifestFileMetaSerializer(RowType partitionType) {
        super(ManifestFileMeta.schema(partitionType));
        this.statsArraySerializer = new FieldStatsArraySerializer(partitionType);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public RowData convertTo(ManifestFileMeta meta) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, StringData.fromString(meta.fileName()));
        row.setField(1, meta.fileSize());
        row.setField(2, meta.numAddedFiles());
        row.setField(3, meta.numDeletedFiles());
        row.setField(4, statsArraySerializer.toRow(meta.partitionStats()));
        return row;
    }

    @Override
    public ManifestFileMeta convertFrom(int version, RowData row) {
        if (version != 1) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        return new ManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                row.getLong(3),
                statsArraySerializer.fromRow(row.getRow(4, statsArraySerializer.numFields())));
    }
}
