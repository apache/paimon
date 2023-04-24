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
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** PartitionEntry Serializer for {@link SimpleManifestEntry}. */
public class SimpleManifestEntrySerializer extends VersionedObjectSerializer<SimpleManifestEntry> {

    private static final long serialVersionUID = 1L;

    public SimpleManifestEntrySerializer() {
        super(SimpleManifestEntry.schema());
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public InternalRow convertTo(SimpleManifestEntry entry) {
        GenericRow row = new GenericRow(5);
        row.setField(0, entry.kind().toByteValue());
        row.setField(1, serializeBinaryRow(entry.partition()));
        row.setField(2, entry.bucket());
        row.setField(3, entry.totalBuckets());
        row.setField(4, GenericRow.of(BinaryString.fromString(entry.fileName()), entry.level()));
        return row;
    }

    @Override
    public SimpleManifestEntry convertFrom(int version, InternalRow row) {
        if (version != 2) {
            if (version == 1) {
                throw new IllegalArgumentException(
                        String.format(
                                "The current version %s is not compatible with the version %s, please recreate the table.",
                                getVersion(), version));
            }
            throw new IllegalArgumentException("Unsupported version: " + version);
        }
        return new SimpleManifestEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                row.getInt(3),
                row.getRow(4, 2).getString(0).toString(),
                row.getRow(4, 2).getInt(1));
    }
}
