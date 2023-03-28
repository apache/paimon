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
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.FollowUpScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.utils.Filter;

/** {@link DataTableScan} for streaming planning. */
public interface StreamDataTableScan extends DataTableScan, InnerStreamTableScan {

    @Override
    StreamDataTableScan withSnapshot(long snapshotId);

    @Override
    StreamDataTableScan withFilter(Predicate predicate);

    @Override
    StreamDataTableScan withKind(ScanKind scanKind);

    @Override
    StreamDataTableScan withLevelFilter(Filter<Integer> levelFilter);

    boolean supportStreamingReadOverwrite();

    StreamDataTableScan withStartingScanner(StartingScanner startingScanner);

    StreamDataTableScan withFollowUpScanner(FollowUpScanner followUpScanner);

    StreamDataTableScan withBoundedChecker(BoundedChecker boundedChecker);

    StreamDataTableScan withSnapshotStarting();
}
