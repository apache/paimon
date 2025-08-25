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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** A {@link TableRead} implementation for {@link FormatTable}. */
public class FormatTableRead implements TableRead {

    private RowType readType;
    private boolean executeFilter = false;
    private Predicate predicate;
    private FormatReadBuilder read;

    public FormatTableRead(RowType readType, FormatReadBuilder read, Predicate predicate) {
        this.readType = readType;
        this.read = read;
        this.predicate = predicate;
    }

    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        return this;
    }

    @Override
    public TableRead executeFilter() {
        this.executeFilter = true;
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        FormatDataSplit dataSplit = (FormatDataSplit) split;
        RecordReader<InternalRow> reader = read.createReader(dataSplit);
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        return reader;
    }

    @Override
    public RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        return TableRead.super.createReader(splits);
    }

    @Override
    public RecordReader<InternalRow> createReader(TableScan.Plan plan) throws IOException {
        return TableRead.super.createReader(plan);
    }

    private RecordReader<InternalRow> executeFilter(RecordReader<InternalRow> reader) {
        if (predicate == null) {
            return reader;
        }

        Predicate predicate = this.predicate;
        if (readType != null) {
            int[] projection = readType.getFieldIndices(readType.getFieldNames());
            Optional<Predicate> optional =
                    predicate.visit(new PredicateProjectionConverter(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(finalFilter::test);
    }
}
