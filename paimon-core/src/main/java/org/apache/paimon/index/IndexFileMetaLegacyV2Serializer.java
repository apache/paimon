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
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** A legacy version serializer for {@link IndexFileMeta}. */
public class IndexFileMetaLegacyV2Serializer extends ObjectSerializer<IndexFileMeta> {

    public IndexFileMetaLegacyV2Serializer() {
        super(schema());
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
                row.getLong(3),
                null);
    }

    private static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_INDEX_TYPE", newStringType(false)));
        fields.add(new DataField(1, "_FILE_NAME", newStringType(false)));
        fields.add(new DataField(2, "_FILE_SIZE", new BigIntType(false)));
        fields.add(new DataField(3, "_ROW_COUNT", new BigIntType(false)));
        return new RowType(fields);
    }
}
