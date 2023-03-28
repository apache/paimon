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

import org.apache.paimon.operation.ScanKind;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.Filter;

import java.io.Serializable;

/** {@link DataTableScan} for batch planning. */
public interface BatchDataTableScan extends DataTableScan {

    @Override
    BatchDataTableScan withSnapshot(long snapshotId);

    @Override
    BatchDataTableScan withFilter(Predicate predicate);

    @Override
    BatchDataTableScan withKind(ScanKind scanKind);

    @Override
    BatchDataTableScan withLevelFilter(Filter<Integer> levelFilter);

    BatchDataTableScan withStartingScanner(StartingScanner startingScanner);

    // ------------------------------------------------------------------------
    //  factory interface
    // ------------------------------------------------------------------------

    /** Factory to create {@link BatchDataTableScan}. */
    interface Factory extends Serializable {
        BatchDataTableScan create(DataTable dataTable);
    }
}
