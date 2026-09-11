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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

/** A factory to create Mosaic reader. */
public class MosaicReaderFactory implements FormatReaderFactory {

    private final RowType dataSchemaRowType;
    private final RowType projectedRowType;
    @Nullable private final List<Predicate> predicates;
    private final int prefetchRowGroups;
    private final long prefetchMaxBytes;

    public MosaicReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            int prefetchRowGroups,
            long prefetchMaxBytes) {
        this.dataSchemaRowType = dataSchemaRowType;
        this.projectedRowType = projectedRowType;
        this.predicates = predicates;
        this.prefetchRowGroups = Math.max(0, prefetchRowGroups);
        this.prefetchMaxBytes = prefetchMaxBytes;
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context) throws IOException {
        // One stream per row group being opened, plus one for the consumer's own reads.
        MosaicInputFileAdapter inputFile =
                new MosaicInputFileAdapter(
                        context.fileIO(), context.filePath(), prefetchRowGroups + 1);
        return new MosaicRecordsReader(
                inputFile,
                context.fileSize(),
                dataSchemaRowType,
                projectedRowType,
                predicates,
                context.filePath(),
                context.selection(),
                prefetchRowGroups,
                prefetchMaxBytes);
    }
}
