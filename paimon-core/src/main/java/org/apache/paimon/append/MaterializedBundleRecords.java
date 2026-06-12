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

package org.apache.paimon.append;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Replayable {@link BundleRecords} backed by copied rows.
 *
 * <p>This is used by dedicated-format fan-out so multiple child writers can consume the same
 * logical bundle without strengthening the public {@link BundleRecords} contract.
 */
class MaterializedBundleRecords implements ReplayableBundleRecords {

    private final List<InternalRow> rows;

    private MaterializedBundleRecords(List<InternalRow> rows) {
        this.rows = rows;
    }

    static MaterializedBundleRecords from(BundleRecords bundle, RowType rowType) {
        InternalRowSerializer serializer = InternalSerializers.create(rowType);
        List<InternalRow> rows =
                new ArrayList<>((int) Math.min(bundle.rowCount(), Integer.MAX_VALUE));
        for (InternalRow row : bundle) {
            rows.add(serializer.copy(row));
        }
        return new MaterializedBundleRecords(rows);
    }

    @Override
    public Iterator<InternalRow> iterator() {
        return rows.iterator();
    }

    @Override
    public long rowCount() {
        return rows.size();
    }
}
