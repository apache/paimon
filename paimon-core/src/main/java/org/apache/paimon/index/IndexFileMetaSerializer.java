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

package org.apache.paimon.index;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.VersionedObjectSerializer;

/** A {@link VersionedObjectSerializer} for {@link IndexFileMeta}. */
public class IndexFileMetaSerializer extends ObjectSerializer<IndexFileMeta> {

    public IndexFileMetaSerializer() {
        super(IndexFileMeta.schema());
    }

    @Override
    public InternalRow toRow(IndexFileMeta record) {
        return GenericRow.of(
                BinaryString.fromString(record.indexType()),
                BinaryString.fromString(record.fileName()),
                record.fileSize(),
                record.rowCount());
    }

    @Override
    public IndexFileMeta fromRow(InternalRow row) {
        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3));
    }
}
