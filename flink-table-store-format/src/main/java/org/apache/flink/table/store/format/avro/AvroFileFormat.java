/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.format.avro;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.format.FileFormat;

/** Avro {@link FileFormat}. */
public class AvroFileFormat extends FileFormat {

    private final org.apache.flink.formats.avro.AvroFileFormatFactory factory;
    private final ReadableConfig formatOptions;

    public AvroFileFormat(ReadableConfig formatOptions) {
        super(org.apache.flink.formats.avro.AvroFileFormatFactory.IDENTIFIER);
        this.factory = new org.apache.flink.formats.avro.AvroFileFormatFactory();
        this.formatOptions = formatOptions;
    }

    @Override
    protected BulkDecodingFormat<RowData> getDecodingFormat() {
        return factory.createDecodingFormat(null, formatOptions); // context is useless
    }

    @Override
    protected EncodingFormat<BulkWriter.Factory<RowData>> getEncodingFormat() {
        return factory.createEncodingFormat(null, formatOptions); // context is useless
    }
}
