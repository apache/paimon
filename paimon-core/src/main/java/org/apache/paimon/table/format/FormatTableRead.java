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
import org.apache.paimon.reader.LimitRecordReader;
import org.apache.paimon.reader.ReadBatchSizer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableReadFilter;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;

/** A {@link TableRead} implementation for {@link FormatTable}. */
public class FormatTableRead implements TableRead {

    private final RowType readType;
    private final RowType tableRowType;
    private final Predicate predicate;
    private final FormatReadBuilder read;
    private final Integer limit;

    private boolean executeFilter = false;
    @Nullable private ReadBatchSizer readBatchSizer;

    public FormatTableRead(
            RowType readType,
            RowType tableRowType,
            FormatReadBuilder read,
            Predicate predicate,
            Integer limit) {
        this.tableRowType = tableRowType;
        this.readType = readType == null ? tableRowType : readType;
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
    public TableRead withReadBatchSizer(ReadBatchSizer sizer) {
        this.readBatchSizer = sizer;
        return this;
    }

    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        FormatDataSplit dataSplit = (FormatDataSplit) split;
        // Capture the binding per TableRead so lazy file suppliers cannot observe another read's
        // sizer.
        ReadBatchSizer sizer = this.readBatchSizer;
        RowType physicalReadType = readType;
        if (executeFilter && predicate != null) {
            physicalReadType = TableReadFilter.readType(tableRowType, readType, predicate);
        }
        RecordReader<InternalRow> reader = read.createReader(dataSplit, sizer, physicalReadType);
        if (executeFilter && predicate != null) {
            reader = TableReadFilter.filter(reader, physicalReadType, predicate);
            reader = TableReadFilter.project(reader, physicalReadType, readType);
        }
        return LimitRecordReader.limit(reader, limit);
    }
}
