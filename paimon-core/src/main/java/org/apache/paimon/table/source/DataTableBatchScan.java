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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;

import java.util.ArrayList;
import java.util.List;

/** {@link TableScan} implementation for batch planning. */
public class DataTableBatchScan extends AbstractDataTableScan {

    private StartingScanner startingScanner;
    private boolean hasNext;

    private Integer pushDownLimit;

    public DataTableBatchScan(
            TableSchema schema,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        super(schema, options, snapshotReader, queryAuth);

        this.hasNext = true;

        if (!schema.primaryKeys().isEmpty() && options.batchScanSkipLevel0()) {
            if (options.toConfiguration()
                    .get(CoreOptions.BATCH_SCAN_MODE)
                    .equals(CoreOptions.BatchScanMode.NONE)) {
                snapshotReader.withLevelFilter(level -> level > 0).enableValueFilter();
            }
        }
        if (options.bucket() == BucketMode.POSTPONE_BUCKET) {
            snapshotReader.onlyReadRealBuckets();
        }
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        super.withFilter(predicate);
        return this;
    }

    @Override
    public InnerTableScan withLimit(int limit) {
        this.pushDownLimit = limit;
        return this;
    }

    @Override
    public TableScan.Plan plan() {
        authQuery();

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

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        if (startingScanner == null) {
            startingScanner = createStartingScanner(false);
        }
        return startingScanner.scanPartitions(snapshotReader);
    }

    private StartingScanner.Result applyPushDownLimit(StartingScanner.Result result) {
        if (pushDownLimit != null && result instanceof ScannedResult) {
            long scannedRowCount = 0;
            SnapshotReader.Plan plan = ((ScannedResult) result).plan();
            List<DataSplit> splits = plan.dataSplits();
            if (splits.isEmpty()) {
                return result;
            }

            List<Split> limitedSplits = new ArrayList<>();
            for (DataSplit dataSplit : splits) {
                if (dataSplit.rawConvertible()) {
                    long partialMergedRowCount = dataSplit.partialMergedRowCount();
                    limitedSplits.add(dataSplit);
                    scannedRowCount += partialMergedRowCount;
                    if (scannedRowCount >= pushDownLimit) {
                        SnapshotReader.Plan newPlan =
                                new PlanImpl(plan.watermark(), plan.snapshotId(), limitedSplits);
                        return new ScannedResult(newPlan);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
