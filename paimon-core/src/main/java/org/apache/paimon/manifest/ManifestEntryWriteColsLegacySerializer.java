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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMetaWriteColsLegacySerializer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.OffsetRow;

import java.util.Arrays;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Legacy serializer for {@link ManifestEntry} before column sequence numbers were introduced. */
public class ManifestEntryWriteColsLegacySerializer extends ObjectSerializer<ManifestEntry> {

    private static final long serialVersionUID = 1L;
    private static final int FORMAT_IDENTIFIER = 2;

    private static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            ManifestEntry.SCHEMA.getField(ManifestEntry.KIND),
                            ManifestEntry.SCHEMA.getField(ManifestEntry.PARTITION),
                            ManifestEntry.SCHEMA.getField(ManifestEntry.BUCKET),
                            ManifestEntry.SCHEMA.getField(ManifestEntry.TOTAL_BUCKETS),
                            ManifestEntry.SCHEMA
                                    .getField(ManifestEntry.FILE)
                                    .newType(DataFileMetaWriteColsLegacySerializer.SCHEMA)));

    private final DataFileMetaWriteColsLegacySerializer dataFileMetaSerializer;

    public ManifestEntryWriteColsLegacySerializer() {
        super(ManifestSchemaUtils.withFormatIdentifier(SCHEMA));
        this.dataFileMetaSerializer = new DataFileMetaWriteColsLegacySerializer();
    }

    @Override
    public InternalRow toRow(ManifestEntry entry) {
        return GenericRow.of(
                FORMAT_IDENTIFIER,
                entry.kind().toByteValue(),
                serializeBinaryRow(entry.partition()),
                entry.bucket(),
                entry.totalBuckets(),
                dataFileMetaSerializer.toRow(entry.file()));
    }

    @Override
    public ManifestEntry fromRow(InternalRow row) {
        ManifestEntrySerializer.checkFormatIdentifier(row.getInt(0));
        InternalRow dataRow = new OffsetRow(row.getFieldCount() - 1, 1).replace(row);
        return ManifestEntry.create(
                FileKind.fromByteValue(dataRow.getByte(0)),
                deserializeBinaryRow(dataRow.getBinary(1)),
                dataRow.getInt(2),
                dataRow.getInt(3),
                dataFileMetaSerializer.fromRow(
                        dataRow.getRow(4, dataFileMetaSerializer.numFields())));
    }
}
