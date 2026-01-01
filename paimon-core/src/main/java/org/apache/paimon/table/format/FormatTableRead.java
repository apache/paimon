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
import org.apache.paimon.predicate.PredicateEvaluator;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** A {@link TableRead} implementation for {@link FormatTable}. */
public class FormatTableRead implements TableRead {

    private final RowType readType;
    private final RowType tableRowType;
    private final Predicate predicate;
    private final FormatReadBuilder read;
    private final Integer limit;

    private boolean executeFilter = false;

    public FormatTableRead(
            RowType readType,
            RowType tableRowType,
            FormatReadBuilder read,
            Predicate predicate,
            Integer limit) {
        this.tableRowType = tableRowType;
        this.readType = readType;
        this.read = read;
        this.predicate = predicate;
        this.limit = limit;
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
        if (limit != null && limit > 0) {
            reader = applyLimit(reader, limit);
        }

        return reader;
    }

    private RecordReader<InternalRow> executeFilter(RecordReader<InternalRow> reader) {
        if (predicate == null) {
            return reader;
        }

        Predicate predicate = this.predicate;
        if (readType != null) {
            int[] projection = tableRowType.getFieldIndices(readType.getFieldNames());
            Optional<Predicate> optional =
                    predicate.visit(new PredicateProjectionConverter(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(row -> PredicateEvaluator.test(finalFilter, row));
    }

    private RecordReader<InternalRow> applyLimit(RecordReader<InternalRow> reader, int limit) {
        return new RecordReader<InternalRow>() {
            private final AtomicLong recordCount = new AtomicLong(0);

            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                if (recordCount.get() >= limit) {
                    return null;
                }
                RecordIterator<InternalRow> iterator = reader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return new RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() throws IOException {
                        if (recordCount.get() >= limit) {
                            return null;
                        }
                        InternalRow next = iterator.next();
                        if (next != null) {
                            recordCount.incrementAndGet();
                        }
                        return next;
                    }

                    @Override
                    public void releaseBatch() {
                        iterator.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
