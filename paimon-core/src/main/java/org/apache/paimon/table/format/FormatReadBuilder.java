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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileRecordReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;
import static org.apache.paimon.predicate.PredicateBuilder.fieldIdxToPartitionIdx;
import static org.apache.paimon.predicate.PredicateBuilder.splitAndByPartition;

/** {@link ReadBuilder} for {@link FormatTable}. */
public class FormatReadBuilder implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private final FormatTable table;
    private RowType readType;
    private final CoreOptions options;
    @Nullable private Predicate filter;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private Integer limit;

    public FormatReadBuilder(FormatTable table) {
        this.table = table;
        this.readType = this.table.rowType();
        this.options = new CoreOptions(table.options());
    }

    @Override
    public String tableName() {
        return this.table.name();
    }

    @Override
    public RowType readType() {
        return this.readType;
    }

    @Override
    public ReadBuilder withFilter(Predicate predicate) {
        if (this.filter == null) {
            this.filter = predicate;
        } else {
            this.filter = PredicateBuilder.and(this.filter, predicate);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            RowType partitionType = table.rowType().project(table.partitionKeys());
            PartitionPredicate partitionPredicate =
                    fromPredicate(
                            partitionType,
                            createPartitionPredicate(
                                    partitionSpec, partitionType, table.defaultPartName()));
            withPartitionFilter(partitionPredicate);
        }
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    @Override
    public ReadBuilder withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    @Override
    public ReadBuilder withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        return withReadType(readType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public TableScan newScan() {
        PartitionPredicate partitionFilter = this.partitionFilter;
        if (partitionFilter == null && this.filter != null && !table.partitionKeys().isEmpty()) {
            int[] partitionIdx = fieldIdxToPartitionIdx(table.rowType(), table.partitionKeys());
            List<Predicate> partitionFilters = splitAndByPartition(filter, partitionIdx).getLeft();
            if (!partitionFilters.isEmpty()) {
                RowType partitionType = table.rowType().project(table.partitionKeys());
                partitionFilter =
                        PartitionPredicate.fromPredicate(
                                partitionType, PredicateBuilder.and(partitionFilters));
            }
        }
        return new FormatTableScan(table, partitionFilter, limit);
    }

    @Override
    public TableRead newRead() {
        return new FormatTableRead(readType(), this, filter, limit);
    }

    protected RecordReader<InternalRow> createReader(FormatDataSplit dataSplit) throws IOException {
        Path filePath = dataSplit.dataPath();
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(table.fileIO(), filePath, dataSplit.length(), null);
        FormatReaderFactory readerFactory =
                FileFormatDiscover.of(options)
                        .discover(options.formatType())
                        .createReaderFactory(
                                table.rowType(), readType(), PredicateBuilder.splitAnd(filter));

        Pair<int[], RowType> partitionMapping =
                PartitionUtils.getPartitionMapping(
                        table.partitionKeys(), readType().getFields(), table.partitionType());

        return new DataFileRecordReader(
                readType(),
                readerFactory,
                formatReaderContext,
                null,
                null,
                PartitionUtils.create(partitionMapping, dataSplit.partition()),
                false,
                null,
                0,
                Collections.emptyMap());
    }

    // ===================== Unsupported ===============================

    @Override
    public ReadBuilder withTopN(TopN topN) {
        return this;
    }

    @Override
    public ReadBuilder dropStats() {
        return this;
    }

    @Override
    public ReadBuilder withBucketFilter(Filter<Integer> bucketFilter) {
        throw new UnsupportedOperationException("Format Table does not support withBucketFilter.");
    }

    @Override
    public ReadBuilder withBucket(int bucket) {
        throw new UnsupportedOperationException("Format Table does not support withBucket.");
    }

    @Override
    public ReadBuilder withShard(int indexOfThisSubtask, int numberOfParallelSubtasks) {
        throw new UnsupportedOperationException("Format Table does not support withShard.");
    }

    @Override
    public StreamTableScan newStreamScan() {
        throw new UnsupportedOperationException("Format Table does not support stream scan.");
    }
}
