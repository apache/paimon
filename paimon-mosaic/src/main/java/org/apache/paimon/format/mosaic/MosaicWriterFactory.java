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

import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/** A factory to create Mosaic {@link FormatWriter}. */
public class MosaicWriterFactory implements FormatWriterFactory {

    private final RowType rowType;
    private final FileFormatFactory.FormatContext formatContext;
    private final List<String> statsColumnNames;
    private final @Nullable Integer numBuckets;

    public MosaicWriterFactory(RowType rowType, FileFormatFactory.FormatContext formatContext) {
        this.rowType = rowType;
        this.formatContext = formatContext;
        String statsColumnsValue = formatContext.options().get(MosaicFileFormat.STATS_COLUMNS);
        if (statsColumnsValue == null || statsColumnsValue.trim().isEmpty()) {
            this.statsColumnNames = new ArrayList<>();
        } else {
            this.statsColumnNames =
                    Arrays.stream(statsColumnsValue.split(","))
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .collect(Collectors.toList());
        }
        this.numBuckets = formatContext.options().get(MosaicFileFormat.NUM_BUCKETS);
    }

    @Override
    public FormatWriter create(PositionOutputStream out, String compression) {
        validateCompression(compression);
        return new MosaicRecordsWriter(out, rowType, formatContext, statsColumnNames, numBuckets);
    }

    private static void validateCompression(String compression) {
        if (compression == null) {
            return;
        }
        String normalized = compression.toLowerCase(Locale.ROOT);
        if (!normalized.equals("zstd")) {
            throw new UnsupportedOperationException(
                    "Mosaic format only supports zstd compression, but got: " + compression);
        }
    }
}
