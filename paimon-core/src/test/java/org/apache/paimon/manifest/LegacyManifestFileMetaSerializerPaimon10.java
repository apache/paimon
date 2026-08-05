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
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.OffsetRow;

import java.util.Arrays;

/** Legacy serializer for {@link ManifestFileMeta} in Paimon 1.0. */
public class LegacyManifestFileMetaSerializerPaimon10 extends ObjectSerializer<ManifestFileMeta> {

    private static final long serialVersionUID = 1L;

    private static final int FORMAT_IDENTIFIER = 2;

    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(
                                    0, "_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)),
                            new DataField(1, "_FILE_SIZE", new BigIntType(false)),
                            new DataField(2, "_NUM_ADDED_FILES", new BigIntType(false)),
                            new DataField(3, "_NUM_DELETED_FILES", new BigIntType(false)),
                            new DataField(4, "_PARTITION_STATS", SimpleStats.SCHEMA),
                            new DataField(5, "_SCHEMA_ID", new BigIntType(false))));

    public LegacyManifestFileMetaSerializerPaimon10() {
        super(ManifestSchemaUtils.withFormatIdentifier(SCHEMA));
    }

    @Override
    public InternalRow toRow(ManifestFileMeta meta) {
        return GenericRow.of(
                FORMAT_IDENTIFIER,
                BinaryString.fromString(meta.fileName()),
                meta.fileSize(),
                meta.numAddedFiles(),
                meta.numDeletedFiles(),
                meta.partitionStats().toRow(),
                meta.schemaId());
    }

    @Override
    public ManifestFileMeta fromRow(InternalRow row) {
        int formatIdentifier = row.getInt(0);
        if (formatIdentifier != FORMAT_IDENTIFIER) {
            if (formatIdentifier == 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "The current version %s is not compatible with the version %s, please recreate the table.",
                                FORMAT_IDENTIFIER, formatIdentifier));
            }
            throw new IllegalArgumentException("Unsupported version: " + formatIdentifier);
        }

        InternalRow data = new OffsetRow(row.getFieldCount() - 1, 1).replace(row);
        return new ManifestFileMeta(
                data.getString(0).toString(),
                data.getLong(1),
                data.getLong(2),
                data.getLong(3),
                SimpleStats.fromRow(data.getRow(4, 3)),
                data.getLong(5),
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
