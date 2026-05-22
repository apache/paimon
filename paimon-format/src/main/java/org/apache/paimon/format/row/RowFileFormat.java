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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
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
        int[] projectionMapping = computeProjectionMapping(dataSchemaRowType, projectedRowType);
        return new RowFormatReaderFactory(dataSchemaRowType, projectionMapping);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new RowFormatWriterFactory(type, blockSize, zstdLevel);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        // RowCompactedSerializer supports all Paimon data types
    }

    @Nullable
    private static int[] computeProjectionMapping(
            RowType dataSchemaRowType, RowType projectedRowType) {
        if (dataSchemaRowType.equals(projectedRowType)) {
            return null;
        }

        List<DataField> dataFields = dataSchemaRowType.getFields();
        List<DataField> projectedFields = projectedRowType.getFields();

        List<Integer> mapping = new ArrayList<>();
        for (DataField projected : projectedFields) {
            for (int i = 0; i < dataFields.size(); i++) {
                if (dataFields.get(i).id() == projected.id()) {
                    mapping.add(i);
                    break;
                }
            }
        }

        int[] result = new int[mapping.size()];
        for (int i = 0; i < mapping.size(); i++) {
            result[i] = mapping.get(i);
        }
        return result;
    }
}
