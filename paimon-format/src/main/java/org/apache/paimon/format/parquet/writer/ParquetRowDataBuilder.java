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
import org.apache.paimon.format.parquet.MapShreddingUtils;
import org.apache.paimon.format.parquet.VariantUtils;
import org.apache.paimon.types.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.convertToParquetMessageType;

/** {@link InternalRow} of {@link ParquetWriter.Builder}. */
public class ParquetRowDataBuilder
        extends ParquetWriter.Builder<InternalRow, ParquetRowDataBuilder> {

    private final RowType rowType;
    private final Map<String, List<String>> dynamicMapKeys;
    @Nullable private final RowType shreddingSchemas;

    public ParquetRowDataBuilder(
            OutputFile path, RowType rowType, @Nullable RowType shreddingSchemas) {
        this(path, rowType, shreddingSchemas, Collections.emptyMap());
    }

    public ParquetRowDataBuilder(
            OutputFile path,
            RowType rowType,
            @Nullable RowType shreddingSchemas,
            Map<String, List<String>> dynamicMapKeys) {
        super(path);
        this.rowType = rowType;
        this.shreddingSchemas = shreddingSchemas;
        this.dynamicMapKeys = dynamicMapKeys;
    }

    @Override
    protected ParquetRowDataBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<InternalRow> getWriteSupport(Configuration conf) {
        return new ParquetWriteSupport(conf);
    }

    private class ParquetWriteSupport extends WriteSupport<InternalRow> {

        private final Configuration conf;
        private final MessageType schema;

        private ParquetRowDataWriter writer;

        private ParquetWriteSupport(Configuration conf) {
            this.conf = conf;
            this.schema =
                    convertToParquetMessageType(
                            VariantUtils.replaceWithShreddingType(rowType, shreddingSchemas),
                            dynamicMapKeys);
        }

        @Override
        public WriteContext init(Configuration configuration) {
            Map<String, String> metadata = new HashMap<>();
            metadata.putAll(MapShreddingUtils.toFooterMetadata(dynamicMapKeys));
            return new WriteContext(schema, metadata);
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.writer =
                    new ParquetRowDataWriter(
                            recordConsumer,
                            rowType,
                            schema,
                            conf,
                            shreddingSchemas,
                            dynamicMapKeys);
        }

        @Override
        public void write(InternalRow record) {
            this.writer.write(record);
        }
    }
}
