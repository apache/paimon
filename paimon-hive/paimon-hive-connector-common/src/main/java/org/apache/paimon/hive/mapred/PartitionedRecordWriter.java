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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.hive.RowDataContainer;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class PartitionedRecordWriter
        implements FileSinkOperator.RecordWriter,
                org.apache.hadoop.mapred.RecordWriter<NullWritable, RowDataContainer> {

    private final PaimonRecordWriter inner;
    private final GenericRow partitionRow;
    private final int dataOnlyWidth;
    private final int fullSchemaWidth;
    private final JoinedRow joinedRow = new JoinedRow();

    PartitionedRecordWriter(
            PaimonRecordWriter inner, GenericRow partitionRow, int fullSchemaWidth) {
        this.inner = inner;
        this.partitionRow = partitionRow;
        this.fullSchemaWidth = fullSchemaWidth;
        this.dataOnlyWidth = fullSchemaWidth - partitionRow.getFieldCount();
    }

    @Override
    public void write(Writable row) throws IOException {
        InternalRow source = ((RowDataContainer) row).get();
        InternalRow toWrite;
        int width = source.getFieldCount();
        if (width == dataOnlyWidth) {
            joinedRow.replace(source, partitionRow);
            toWrite = joinedRow;
        } else if (width == fullSchemaWidth) {
            toWrite = source;
        } else {
            throw new IOException(
                    "Unexpected row width "
                            + width
                            + "; expected "
                            + dataOnlyWidth
                            + " (static partition path) or "
                            + fullSchemaWidth
                            + " (full schema)");
        }
        try {
            inner.batchTableWrite().write(toWrite);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public void write(NullWritable key, RowDataContainer value) throws IOException {
        write(value);
    }

    @Override
    public void close(boolean abort) throws IOException {
        inner.close(abort);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        inner.close(reporter);
    }
}
