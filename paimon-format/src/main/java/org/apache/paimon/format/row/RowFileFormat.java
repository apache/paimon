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

package org.apache.paimon.format.row;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.NestedProjectedRow;

import javax.annotation.Nullable;

import java.util.List;

/** Row-store file format with block-level ZSTD compression and O(1) row-number lookup. */
public class RowFileFormat extends FileFormat {

    private static final int DEFAULT_BLOCK_SIZE = 65536;

    private final int blockSize;
    private final int zstdLevel;

    public RowFileFormat(FormatContext formatContext) {
        super(RowFileFormatFactory.IDENTIFIER);
        this.zstdLevel = formatContext.zstdLevel();
        MemorySize bs = formatContext.blockSize();
        this.blockSize = bs != null ? (int) bs.getBytes() : DEFAULT_BLOCK_SIZE;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        NestedProjectedRow projection =
                NestedProjectedRow.create(dataSchemaRowType, projectedRowType);
        return new RowFormatReaderFactory(dataSchemaRowType, projection);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new RowFormatWriterFactory(type, blockSize, zstdLevel);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        // Row format supports all Paimon data types
    }
}
