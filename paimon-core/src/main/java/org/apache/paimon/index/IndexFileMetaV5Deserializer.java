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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/** Reads the seven-field index metadata used by commit message versions 12 through 14. */
public class IndexFileMetaV5Deserializer extends ObjectSerializer<IndexFileMeta> {
    public IndexFileMetaV5Deserializer() {
        super(new RowType(false, IndexFileMeta.SCHEMA.getFields().subList(0, 7)));
    }

    @Override
    public InternalRow toRow(IndexFileMeta record) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexFileMeta fromRow(InternalRow row) {
        return new IndexFileMetaSerializer().fromRow(row);
    }
}
