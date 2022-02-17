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

package org.apache.flink.table.store.file.utils;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.mergetree.sst.SstFileTest;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;

/** A special avro {@link FileFormat} which flushes for every added element. */
public class FlushingAvroFormat implements FileFormat {

    private final FileFormat avro =
            FileFormat.fromIdentifier(
                    SstFileTest.class.getClassLoader(), "avro", new Configuration());

    @Override
    public BulkFormat<RowData, FileSourceSplit> createReaderFactory(
            RowType type, List<ResolvedExpression> filters) {
        return avro.createReaderFactory(type, filters);
    }

    @Override
    public BulkWriter.Factory<RowData> createWriterFactory(RowType type) {
        return fsDataOutputStream -> {
            BulkWriter<RowData> wrapped = avro.createWriterFactory(type).create(fsDataOutputStream);
            return new BulkWriter<RowData>() {
                @Override
                public void addElement(RowData rowData) throws IOException {
                    wrapped.addElement(rowData);
                    wrapped.flush();
                }

                @Override
                public void flush() throws IOException {
                    wrapped.flush();
                }

                @Override
                public void finish() throws IOException {
                    wrapped.finish();
                }
            };
        };
    }
}
