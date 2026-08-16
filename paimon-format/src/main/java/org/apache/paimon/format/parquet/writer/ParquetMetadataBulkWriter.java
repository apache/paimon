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
import org.apache.paimon.format.SupportsWriterMetadata;

import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A {@link ParquetBulkWriter} that supports adding file metadata before close. */
public class ParquetMetadataBulkWriter extends ParquetBulkWriter implements SupportsWriterMetadata {

    private final Map<String, byte[]> metadata;

    private boolean closed = false;

    public ParquetMetadataBulkWriter(
            ParquetWriter<InternalRow> parquetWriter, Map<String, byte[]> metadata) {
        super(parquetWriter);
        this.metadata = checkNotNull(metadata, "metadata");
    }

    @Override
    public void addMetadata(Map<String, byte[]> metadata) {
        checkState(!closed, "Cannot add metadata after writer is closed.");
        for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
            this.metadata.put(
                    entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));
        }
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        super.close();
    }
}
