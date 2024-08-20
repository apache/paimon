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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;

import java.util.List;

/** An {@link InnerTableScan} for reading only once, this is for batch scan. */
public abstract class ReadOnceTableScan implements InnerTableScan {

    private boolean hasNext = true;

    @Override
    public Plan plan() {
        if (hasNext) {
            hasNext = false;
            return innerPlan();
        } else {
            throw new EndOfScanException();
        }
    }

    protected abstract Plan innerPlan();

    @Override
    public List<BinaryRow> listPartitions() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionEntry> getPartitionEntries() {
        throw new UnsupportedOperationException();
    }
}
