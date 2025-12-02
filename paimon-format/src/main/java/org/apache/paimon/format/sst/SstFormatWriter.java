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

package org.apache.paimon.format.sst;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.sst.SstFileWriter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;

/** The {@link FormatWriter} for SST file. */
public class SstFormatWriter implements FormatWriter {
    private final SstFileWriter fileWriter;
    private final ProjectedRow reusedKey;
    private final ProjectedRow reusedValue;
    private final RowCompactedSerializer keySerializer;
    private final RowCompactedSerializer valueSerializer;

    public SstFormatWriter(
            PositionOutputStream out,
            String compression,
            long blockSize,
            @Nullable BloomFilter.Builder bloomFilter,
            RowType dataType,
            RowType keyType,
            RowType valueType) {
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(
                        BlockCompressionType.getCompressionTypeByValue(compression));
        this.fileWriter = new SstFileWriter(out, (int) blockSize, bloomFilter, compressionFactory);
        this.reusedKey = ProjectedRow.from(keyType, dataType);
        this.reusedValue = ProjectedRow.from(valueType, dataType);
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueSerializer = new RowCompactedSerializer(valueType);
    }

    @Override
    public void addElement(InternalRow element) throws IOException {
        reusedKey.replaceRow(element);
        reusedValue.replaceRow(element);
        fileWriter.put(
                keySerializer.serializeToBytes(reusedKey),
                valueSerializer.serializeToBytes(reusedValue));
    }

    @Override
    public boolean reachTargetSize(boolean suggestedCheck, long targetSize) throws IOException {
        return suggestedCheck && fileWriter.getTotalCompressedSize() >= targetSize;
    }

    @Override
    public void close() throws IOException {
        fileWriter.close();
    }
}
