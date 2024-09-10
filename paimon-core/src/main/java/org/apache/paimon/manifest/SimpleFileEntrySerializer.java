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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.VersionedObjectSerializer;

import static org.apache.paimon.utils.InternalRowUtils.fromStringArrayData;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** A {@link VersionedObjectSerializer} for {@link SimpleFileEntry}, only supports reading. */
public class SimpleFileEntrySerializer extends VersionedObjectSerializer<SimpleFileEntry> {

    private static final long serialVersionUID = 1L;

    private final int version;

    public SimpleFileEntrySerializer() {
        super(ManifestEntry.SCHEMA);
        this.version = new ManifestEntrySerializer().getVersion();
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public InternalRow convertTo(SimpleFileEntry meta) {
        throw new UnsupportedOperationException("Only supports convert from row.");
    }

    @Override
    public SimpleFileEntry convertFrom(int version, InternalRow row) {
        if (this.version != version) {
            throw new IllegalArgumentException("Unsupported version: " + version);
        }

        InternalRow file = row.getRow(4, DataFileMeta.SCHEMA.getFieldCount());
        return new SimpleFileEntry(
                FileKind.fromByteValue(row.getByte(0)),
                deserializeBinaryRow(row.getBinary(1)),
                row.getInt(2),
                file.getInt(10),
                file.getString(0).toString(),
                fromStringArrayData(file.getArray(11)),
                file.isNullAt(14) ? null : file.getBinary(14),
                deserializeBinaryRow(file.getBinary(3)),
                deserializeBinaryRow(file.getBinary(4)));
    }
}
