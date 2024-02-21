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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.operation.DefaultValueAssigner;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** {@link TableScan} implementation for batch planning. */
public class InnerTableScanImpl extends AbstractInnerTableScan {

    private final SnapshotManager snapshotManager;
    private final DefaultValueAssigner defaultValueAssigner;

    private StartingScanner startingScanner;
    private boolean hasNext;

    private Integer pushDownLimit;

    public InnerTableScanImpl(
            CoreOptions options,
            SnapshotReader snapshotReader,
            SnapshotManager snapshotManager,
            DefaultValueAssigner defaultValueAssigner) {
        super(options, snapshotReader);
        this.snapshotManager = snapshotManager;
        this.hasNext = true;
        this.defaultValueAssigner = defaultValueAssigner;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        snapshotReader.withFilter(defaultValueAssigner.handlePredicate(predicate));
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        this.pushDownLimit = limit;
        return this;
    }

    @Override
    public TableScan.Plan plan() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }

        if (hasNext) {
            hasNext = false;
            StartingScanner.Result result = startingScanner.scan(snapshotReader);
            StartingScanner.Result limitedResult = applyPushDownLimit(result);
            return DataFilePlan.fromResult(limitedResult);
        } else {
            throw new EndOfScanException();
        }
    }

    private StartingScanner.Result applyPushDownLimit(StartingScanner.Result result) {
        if (pushDownLimit != null && result instanceof ScannedResult) {
            long scannedRowCount = 0;
            SnapshotReader.Plan plan = ((ScannedResult) result).plan();
            List<DataSplit> splits = plan.dataSplits();
            List<Split> limitedSplits = new ArrayList<>();
            for (DataSplit dataSplit : splits) {
                long splitRowCount = getRowCountForSplit(dataSplit);
                limitedSplits.add(dataSplit);
                scannedRowCount += splitRowCount;
                if (scannedRowCount >= pushDownLimit) {
                    break;
                }
            }

            SnapshotReader.Plan newPlan =
                    new SnapshotReader.Plan() {
                        @Nullable
                        @Override
                        public Long watermark() {
                            return plan.watermark();
                        }

                        @Nullable
                        @Override
                        public Long snapshotId() {
                            return plan.snapshotId();
                        }

                        @Override
                        public List<Split> splits() {
                            return limitedSplits;
                        }
                    };
            return new ScannedResult(newPlan);
        } else {
            return result;
        }
    }

    /**
     * 0 represents that we can't compute the row count of this split, 'cause this split needs to be
     * merged.
     */
    private long getRowCountForSplit(DataSplit split) {
        if (split.convertToRawFiles().isPresent()) {
            return split.convertToRawFiles().get().stream()
                    .map(RawFile::rowCount)
                    .reduce(Long::sum)
                    .orElse(0L);
        } else {
            return 0L;
        }
    }
}
