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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.OffsetRow;

import java.util.function.Function;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Serializer for {@link ManifestEntry}. */
public class ManifestEntrySerializer extends ObjectSerializer<ManifestEntry> {

    private static final long serialVersionUID = 1L;

    /**
     * Permanent on-disk format identifier, not a schema version.
     *
     * <p>Do not change when adding nullable fields. Old manifest readers skip unknown fields.
     */
    private static final int FORMAT_IDENTIFIER = 2;

    private final DataFileMetaSerializer dataFileMetaSerializer;

    public ManifestEntrySerializer() {
        super(ManifestSchemaUtils.withFormatIdentifier(ManifestEntry.SCHEMA));
        this.dataFileMetaSerializer = new DataFileMetaSerializer();
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

    private ManifestEntry fromDataRow(InternalRow row) {
        return ManifestEntry.create(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                row.getInt(3),
                dataFileMetaSerializer.fromRow(row.getRow(4, dataFileMetaSerializer.numFields())));
    }

    public static Function<InternalRow, FileKind> kindGetter() {
        return row -> FileKind.fromByteValue(row.getByte(1));
    }

    public static Function<InternalRow, BinaryRow> partitionGetter() {
        return row -> deserializeBinaryRow(row.getBinary(2));
    }

    public static Function<InternalRow, Integer> bucketGetter() {
        return row -> row.getInt(3);
    }

    public static Function<InternalRow, Integer> totalBucketGetter() {
        return row -> row.getInt(4);
    }

    public static Function<InternalRow, String> fileNameGetter() {
        return row -> row.getRow(5, DataFileMeta.SCHEMA.getFieldCount()).getString(0).toString();
    }

    public static Function<InternalRow, Integer> levelGetter() {
        return row -> row.getRow(5, DataFileMeta.SCHEMA.getFieldCount()).getInt(10);
    }
}
