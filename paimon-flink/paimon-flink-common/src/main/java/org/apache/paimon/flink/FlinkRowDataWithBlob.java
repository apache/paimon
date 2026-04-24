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

package org.apache.paimon.flink;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.InternalRow;

import java.util.Set;

/** Convert to Flink row data with blob. */
public class FlinkRowDataWithBlob extends FlinkRowData {

    private final Set<Integer> blobFields;
    private final boolean blobAsDescriptor;

    public FlinkRowDataWithBlob(
            InternalRow row, Set<Integer> blobFields, boolean blobAsDescriptor) {
        super(row);
        this.blobFields = blobFields;
        this.blobAsDescriptor = blobAsDescriptor;
    }

    @Override
    public byte[] getBinary(int pos) {
        if (blobFields.contains(pos)) {
            Blob blob = row.getBlob(pos);
            return blobAsDescriptor ? blob.toDescriptor().serialize() : blob.toData();
        } else {
            return row.getBinary(pos);
        }
    }
}
