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
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.ReaderSupplier;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.partition.PartitionPredicate.createPartitionPredicate;
import static org.apache.paimon.partition.PartitionPredicate.fromPredicate;

/** {@link ReadBuilder} for {@link FormatTable}. */
public class FormatReadBuilder implements ReadBuilder {

    private static final long serialVersionUID = 1L;

    private CoreOptions options;
    private FormatTable table;
    private RowType readType;

    private final FileFormatDiscover formatDiscover;

    @Nullable private List<Predicate> filters;

    private @Nullable PartitionPredicate partitionFilter;
    private @Nullable Predicate predicate;
    private @Nullable int[] projection;

    public FormatReadBuilder(FormatTable table) {
        this.table = table;
        this.readType = this.table.rowType();
        this.options = new CoreOptions(table.options());
        this.formatDiscover = FileFormatDiscover.of(this.options);
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
        this.predicate = predicate;
        return this;
    }

    @Override
    public ReadBuilder withPartitionFilter(Map<String, String> partitionSpec) {
        if (partitionSpec != null) {
            RowType partitionType = readType().project(table.partitionKeys());
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
        this.projection = projection;
        return withReadType(readType().project(projection));
    }

    @Override
    public ReadBuilder withLimit(int limit) {
        return this;
    }

    @Override
    public ReadBuilder withTopN(TopN topN) {
        throw new UnsupportedOperationException("TopN is not supported for FormatTable.");
    }

    @Override
    public TableScan newScan() {
        FormatTableScan scan = new FormatTableScan(table, predicate, projection);
        scan.withPartitionFilter(partitionFilter);
        return scan;
    }

    @Override
    public TableRead newRead() {
        FormatReadBuilder read = this;
        return new FormatTableRead(readType(), read, predicate);
    }

    RecordReader<InternalRow> createReader(FormatDataSplit dataSplit) throws IOException {
        List<ReaderSupplier<InternalRow>> suppliers = new ArrayList<>();

        String formatIdentifier = options.formatType();
        suppliers.add(() -> createFileReader(dataSplit, formatIdentifier));

        return ConcatRecordReader.create(suppliers);
    }

    private FileRecordReader<InternalRow> createFileReader(
            FormatDataSplit dataSplit, String formatIdentifier) throws IOException {

        Path filePath = dataSplit.dataPath();
        FormatReaderContext formatReaderContext =
                new FormatReaderContext(table.fileIO(), filePath, dataSplit.length(), null);

        // Create FormatReaderFactory directly
        FormatReaderFactory readerFactory =
                formatDiscover.discover(formatIdentifier).createReaderFactory(readType(), filters);
        Pair<int[], RowType> partitionMapping =
                PartitionUtils.getPartitionMapping(
                        table.partitionKeys(), table.rowType().getFields(), table.partitionType());
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        readType(),
                        readerFactory,
                        formatReaderContext,
                        null, // indexMapping
                        null, // castMapping
                        PartitionUtils.create(partitionMapping, dataSplit.partition()),
                        false,
                        null,
                        0,
                        Collections.emptyMap()); // systemFields
        return fileRecordReader;
    }

    // ===================== Unsupported ===============================

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
