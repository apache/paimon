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

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Mosaic file format: a column-bucket hybrid format optimized for wide tables (1,000-100,000+
 * columns). Columns are hashed into buckets, row-stored within each bucket, and independently
 * compressed. Projection pushdown works at bucket granularity.
 */
public class MosaicFileFormat extends FileFormat {

    private final int numBuckets;
    private final int zstdLevel;
    private final long rowGroupMaxSize;
    private final int maxDictTotalBytes;
    private final int maxDictEntries;

    public MosaicFileFormat(FormatContext formatContext) {
        super(MosaicFileFormatFactory.IDENTIFIER);
        this.numBuckets = formatContext.options().get(MosaicOptions.NUM_COLUMN_BUCKETS);
        this.zstdLevel = formatContext.zstdLevel();
        this.rowGroupMaxSize = formatContext.writeBatchMemory().getBytes();
        this.maxDictTotalBytes = formatContext.options().get(MosaicOptions.DICT_MAX_TOTAL_BYTES);
        this.maxDictEntries = formatContext.options().get(MosaicOptions.DICT_MAX_ENTRIES);
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new MosaicReaderFactory(projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new MosaicWriterFactory(
                type, numBuckets, zstdLevel, rowGroupMaxSize, maxDictTotalBytes, maxDictEntries);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        rowType.getFields().forEach(f -> validateFieldType(f.type().getTypeRoot(), f.name()));
    }

    private static void validateFieldType(DataTypeRoot root, String fieldName) {
        switch (root) {
            case ARRAY:
            case VECTOR:
            case MAP:
            case MULTISET:
            case ROW:
            case VARIANT:
            case BLOB:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + root + " for field: " + fieldName);
            default:
        }
    }
}
