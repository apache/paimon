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
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.utils.ProjectedRow;

import java.util.Iterator;

/** Bundle wrapper which applies projection lazily during iteration. */
class ProjectedBundleRecords implements ReplayableBundleRecords {

    private final ReplayableBundleRecords bundle;
    private final int[] projection;

    ProjectedBundleRecords(ReplayableBundleRecords bundle, int[] projection) {
        this.bundle = bundle;
        this.projection = projection;
    }

    @Override
    public Iterator<InternalRow> iterator() {
        final Iterator<InternalRow> iterator = bundle.iterator();
        final ProjectedRow projectedRow = ProjectedRow.from(projection);
        return new Iterator<InternalRow>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public InternalRow next() {
                return projectedRow.replaceRow(iterator.next());
            }
        };
    }

    @Override
    public long rowCount() {
        return bundle.rowCount();
    }
}
