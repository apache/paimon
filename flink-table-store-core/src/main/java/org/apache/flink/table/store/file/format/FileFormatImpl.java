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

package org.apache.flink.table.store.file.format;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;

/** A {@link FileFormat} which discovers reader and writer from format identifier. */
public class FileFormatImpl extends FileFormat {

    private final BulkReaderFormatFactory readerFactory;
    private final BulkWriterFormatFactory writerFactory;
    private final ReadableConfig formatOptions;

    public FileFormatImpl(
            ClassLoader classLoader, String formatIdentifier, ReadableConfig formatOptions) {
        super(formatIdentifier);
        this.readerFactory =
                FactoryUtil.discoverFactory(
                        classLoader, BulkReaderFormatFactory.class, formatIdentifier);
        this.writerFactory =
                FactoryUtil.discoverFactory(
                        classLoader, BulkWriterFormatFactory.class, formatIdentifier);
        this.formatOptions = formatOptions;
    }

    protected BulkDecodingFormat<RowData> getDecodingFormat() {
        return readerFactory.createDecodingFormat(null, formatOptions); // context is useless
    }

    @Override
    protected EncodingFormat<BulkWriter.Factory<RowData>> getEncodingFormat() {
        return writerFactory.createEncodingFormat(null, formatOptions); // context is useless
    }
}
