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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.OffsetRow;

/** Serializer for {@link ManifestFileMeta}. */
public class ManifestFileMetaSerializer extends ObjectSerializer<ManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    /**
     * Permanent on-disk format identifier, not a schema version.
     *
     * <p>Do not change when adding nullable fields. Old manifest readers skip unknown fields.
     */
    private static final int FORMAT_IDENTIFIER = 2;

    public ManifestFileMetaSerializer() {
        super(ManifestSchemaUtils.withFormatIdentifier(ManifestFileMeta.SCHEMA));
    }

    @Override
    public InternalRow toRow(ManifestFileMeta meta) {
        return new JoinedRow().replace(GenericRow.of(FORMAT_IDENTIFIER), toDataRow(meta));
    }

    private InternalRow toDataRow(ManifestFileMeta meta) {
        return GenericRow.of(
                BinaryString.fromString(meta.fileName()),
                meta.fileSize(),
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.partitionStats().toRow(),
                meta.schemaId(),
                meta.minBucket(),
                meta.maxBucket(),
                meta.minLevel(),
                meta.maxLevel(),
                meta.minRowId(),
                meta.maxRowId());
    }

    @Override
    public ManifestFileMeta fromRow(InternalRow row) {
        checkFormatIdentifier(row.getInt(0));
        return fromDataRow(new OffsetRow(row.getFieldCount() - 1, 1).replace(row));
    }

    private void checkFormatIdentifier(int formatIdentifier) {
        if (formatIdentifier != FORMAT_IDENTIFIER) {
            if (formatIdentifier == 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "The current version %s is not compatible with the version %s, please recreate the table.",
                                FORMAT_IDENTIFIER, formatIdentifier));
            }
            throw new IllegalArgumentException("Unsupported version: " + formatIdentifier);
        }
    }

    private ManifestFileMeta fromDataRow(InternalRow row) {
        return new ManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                row.getLong(3),
                SimpleStats.fromRow(row.getRow(4, 3)),
                row.getLong(5),
                row.isNullAt(6) ? null : row.getInt(6),
                row.isNullAt(7) ? null : row.getInt(7),
                row.isNullAt(8) ? null : row.getInt(8),
                row.isNullAt(9) ? null : row.getInt(9),
                row.isNullAt(10) ? null : row.getLong(10),
                row.isNullAt(11) ? null : row.getLong(11));
    }
}
