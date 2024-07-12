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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link IcebergDataFileMeta}. */
public class IcebergDataFileMetaSerializer extends ObjectSerializer<IcebergDataFileMeta> {

    private static final long serialVersionUID = 1L;

    private final InternalRowSerializer partSerializer;

    public IcebergDataFileMetaSerializer(RowType partitionType) {
        super(IcebergDataFileMeta.schema(partitionType));
        this.partSerializer = new InternalRowSerializer(partitionType);
    }

    @Override
    public InternalRow toRow(IcebergDataFileMeta file) {
        return GenericRow.of(
                file.content().id(),
                BinaryString.fromString(file.filePath()),
                BinaryString.fromString(file.fileFormat()),
                file.partition(),
                file.recordCount(),
                file.fileSizeInBytes());
    }

    @Override
    public IcebergDataFileMeta fromRow(InternalRow row) {
        return new IcebergDataFileMeta(
                IcebergDataFileMeta.Content.fromId(row.getInt(0)),
                row.getString(1).toString(),
                row.getString(2).toString(),
                partSerializer.toBinaryRow(row.getRow(3, partSerializer.getArity())).copy(),
                row.getLong(4),
                row.getLong(5));
    }
}
