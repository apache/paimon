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
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StartingScanner.ScannedResult;
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;

/** {@link TableScan} implementation for batch planning. */
public class DataTableBatchScan extends AbstractDataTableScan {

    private StartingScanner startingScanner;
    private boolean hasNext;

    private Integer pushDownLimit;
    private TopN topN;

    private final SchemaManager schemaManager;

    public DataTableBatchScan(
            TableSchema schema,
            SchemaManager schemaManager,
            CoreOptions options,
            SnapshotReader snapshotReader,
            TableQueryAuth queryAuth) {
        super(schema, options, snapshotReader, queryAuth);

        this.hasNext = true;
        this.schemaManager = schemaManager;
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
        snapshotReader.withLimit(limit);
        return this;
    }

    @Override
    public InnerTableScan withTopN(TopN topN) {
        this.topN = topN;
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
            Optional<StartingScanner.Result> pushed = applyPushDownLimit();
            if (pushed.isPresent()) {
                return DataFilePlan.fromResult(pushed.get());
            }
            pushed = applyPushDownTopN();
            if (pushed.isPresent()) {
                return DataFilePlan.fromResult(pushed.get());
            }
            return DataFilePlan.fromResult(startingScanner.scan(snapshotReader));
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

    private Optional<StartingScanner.Result> applyPushDownLimit() {
        if (pushDownLimit == null) {
            return Optional.empty();
        }

        StartingScanner.Result result = startingScanner.scan(snapshotReader);
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        long scannedRowCount = 0;
        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<DataSplit> splits = plan.dataSplits();
        if (splits.isEmpty()) {
            return Optional.of(result);
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
                    return Optional.of(new ScannedResult(newPlan));
                }
            }
        }
        return Optional.of(result);
    }

    private Optional<StartingScanner.Result> applyPushDownTopN() {
        if (topN == null
                || pushDownLimit != null
                || !schema.primaryKeys().isEmpty()
                || options().deletionVectorsEnabled()) {
            return Optional.empty();
        }

        List<SortValue> orders = topN.orders();
        if (orders.size() != 1) {
            return Optional.empty();
        }

        if (topN.limit() > 100) {
            return Optional.empty();
        }

        SortValue order = orders.get(0);
        DataType type = order.field().type();
        if (!minmaxAvailable(type)) {
            return Optional.empty();
        }

        StartingScanner.Result result = startingScanner.scan(snapshotReader.keepStats());
        if (!(result instanceof ScannedResult)) {
            return Optional.of(result);
        }

        SnapshotReader.Plan plan = ((ScannedResult) result).plan();
        List<DataSplit> splits = plan.dataSplits();
        if (splits.isEmpty()) {
            return Optional.of(result);
        }

        TopNDataSplitEvaluator evaluator = new TopNDataSplitEvaluator(schema, schemaManager);
        List<Split> topNSplits = new ArrayList<>(evaluator.evaluate(order, topN.limit(), splits));
        SnapshotReader.Plan newPlan = new PlanImpl(plan.watermark(), plan.snapshotId(), topNSplits);
        return Optional.of(new ScannedResult(newPlan));
    }

    @Override
    public DataTableScan withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        snapshotReader.withShard(indexOfThisSubtask, numberOfParallelSubtasks);
        return this;
    }
}
