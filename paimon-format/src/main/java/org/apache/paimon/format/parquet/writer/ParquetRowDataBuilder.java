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

package org.apache.paimon.format.parquet.writer;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;

/** {@link InternalRow} of {@link ParquetWriter.Builder}. */
public class ParquetRowDataBuilder
        extends ParquetWriter.Builder<InternalRow, ParquetRowDataBuilder> {

    private final RowType rowType;

    public ParquetRowDataBuilder(OutputFile path, RowType rowType) {
        super(path);
        this.rowType = rowType;
    }

    @Override
    protected ParquetRowDataBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<InternalRow> getWriteSupport(Configuration conf) {
        return new ParquetWriteSupport();
    }

    private class ParquetWriteSupport extends WriteSupport<InternalRow> {

        private final MessageType schema = convertToParquetMessageType(rowType);

        private ParquetRowDataWriter writer;

        @Override
        public WriteContext init(Configuration configuration) {
            return new WriteContext(schema, new HashMap<>());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.writer = new ParquetRowDataWriter(recordConsumer, rowType, schema);
        }

        @Override
        public void write(InternalRow record) {
            try {
                this.writer.write(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
