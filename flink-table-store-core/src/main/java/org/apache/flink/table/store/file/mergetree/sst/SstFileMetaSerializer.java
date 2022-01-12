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

package org.apache.flink.table.store.file.mergetree.sst;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.stats.FieldStatsArraySerializer;
import org.apache.flink.table.store.file.utils.ObjectSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Serializer for {@link SstFileMeta}. */
public class SstFileMetaSerializer extends ObjectSerializer<SstFileMeta> {

    private final RowDataSerializer keySerializer;
    private final FieldStatsArraySerializer statsArraySerializer;

    public SstFileMetaSerializer(RowType keyType, RowType rowType) {
        super(SstFileMeta.schema(keyType, rowType));
        this.keySerializer = new RowDataSerializer(keyType);
        this.statsArraySerializer = new FieldStatsArraySerializer(rowType);
    }

    @Override
    public RowData toRow(SstFileMeta meta) {
        return GenericRowData.of(
                StringData.fromString(meta.fileName()),
                meta.fileSize(),
                meta.rowCount(),
                meta.minKey(),
                meta.maxKey(),
                statsArraySerializer.toRow(meta.stats()),
                meta.minSequenceNumber(),
                meta.maxSequenceNumber(),
                meta.level());
    }

    @Override
    public SstFileMeta fromRow(RowData row) {
        int keyFieldCount = keySerializer.getArity();
        return new SstFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                keySerializer.toBinaryRow(row.getRow(3, keyFieldCount)).copy(),
                keySerializer.toBinaryRow(row.getRow(4, keyFieldCount)).copy(),
                statsArraySerializer.fromRow(row.getRow(5, statsArraySerializer.numFields())),
                row.getLong(6),
                row.getLong(7),
                row.getInt(8));
    }
}
