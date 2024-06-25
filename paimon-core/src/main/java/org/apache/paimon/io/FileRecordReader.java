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

package org.apache.paimon.io;

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;

/** Reads {@link InternalRow} from data files. */
public class FileRecordReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> reader;
    @Nullable private final int[] indexMapping;
    @Nullable private final PartitionInfo partitionInfo;
    @Nullable private final CastFieldGetter[] castMapping;

    @Nullable
    @Override
    public RecordReader.RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        if (iterator instanceof ColumnarRowIterator) {
            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo, indexMapping);
        } else {
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                iterator = iterator.transform(partitionSettedRow::replaceRow);
            }
            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                iterator = iterator.transform(projectedRow::replaceRow);
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        return iterator;
    }

    public FileRecordReader(
            FormatReaderFactory readerFactory,
            FormatReaderFactory.Context context,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo)
            throws IOException {
        try {
            this.reader = readerFactory.createReader(context);
        } catch (Exception e) {
            FileUtils.checkExists(context.fileIO(), context.filePath());
            throw e;
        }
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
