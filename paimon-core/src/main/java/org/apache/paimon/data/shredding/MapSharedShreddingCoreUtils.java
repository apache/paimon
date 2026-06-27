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

package org.apache.paimon.data.shredding;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.SupportsFieldMetadata;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Core utilities for shared-shredding MAP write and restore flows. */
public class MapSharedShreddingCoreUtils {

    private MapSharedShreddingCoreUtils() {}

    @Nullable
    public static String fieldIdMetadataKey(FileFormat fileFormat) {
        switch (fileFormat.getFormatIdentifier()) {
            case "parquet":
                return FormatMetadataUtils.PARQUET_FIELD_ID_KEY;
            case "orc":
                return FormatMetadataUtils.ORC_FIELD_ID_KEY;
            default:
                return null;
        }
    }

    @Nullable
    public static MapSharedShreddingContext createAndRestoreContext(
            RowType writeType,
            List<DataFileMeta> restoredFiles,
            DataFilePathFactory pathFactory,
            CoreOptions options,
            FileIO fileIO) {
        List<String> shreddingFieldNames =
                MapSharedShreddingUtils.detectShreddingColumns(writeType, options);
        if (shreddingFieldNames.isEmpty()) {
            return null;
        }

        MapSharedShreddingContext context =
                new MapSharedShreddingContext(
                        MapSharedShreddingUtils.buildColumnToNumColumns(
                                shreddingFieldNames, options));
        restoreRecentFileStats(
                context,
                shreddingFieldNames,
                restoredFiles,
                pathFactory,
                fileIO,
                FileFormatDiscover.of(options));
        return context;
    }

    private static void restoreRecentFileStats(
            MapSharedShreddingContext context,
            List<String> shreddingFieldNames,
            List<DataFileMeta> restoredFiles,
            DataFilePathFactory pathFactory,
            FileIO fileIO,
            FileFormatDiscover fileFormatDiscover) {
        Set<String> pendingFieldNames = new HashSet<>(shreddingFieldNames);
        if (pendingFieldNames.isEmpty()) {
            return;
        }

        for (int i = restoredFiles.size() - 1; i >= 0 && !pendingFieldNames.isEmpty(); i--) {
            DataFileMeta file = restoredFiles.get(i);
            List<String> candidateFields =
                    candidateFields(file.writeCols(), shreddingFieldNames, pendingFieldNames);
            if (candidateFields.isEmpty()) {
                continue;
            }

            FileFormat fileFormat = fileFormatDiscover.discover(file.fileFormat());
            if (!(fileFormat instanceof SupportsFieldMetadata)) {
                continue;
            }
            try {
                Map<String, Map<String, String>> fieldMetadata =
                        ((SupportsFieldMetadata) fileFormat)
                                .readFieldMetadata(
                                        new FormatReaderContext(
                                                fileIO,
                                                pathFactory.toPath(file),
                                                file.fileSize()));
                for (String fieldName : candidateFields) {
                    Map<String, String> metadata = fieldMetadata.get(fieldName);
                    if (!MapSharedShreddingUtils.hasShreddingMetadata(metadata)) {
                        continue;
                    }

                    MapSharedShreddingFieldMeta fieldMeta =
                            MapSharedShreddingUtils.deserializeMetadata(
                                    metadata, MapSharedShreddingDefine.DEFAULT_DICT_COMPRESSION);
                    context.reportFileStats(fieldName, fieldMeta.maxRowWidth());
                    pendingFieldNames.remove(fieldName);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static List<String> candidateFields(
            @Nullable List<String> writeCols,
            List<String> shreddingFieldNames,
            Set<String> pendingFieldNames) {
        List<String> fields = new ArrayList<>();
        for (String fieldName : shreddingFieldNames) {
            if (pendingFieldNames.contains(fieldName)
                    && (writeCols == null || writeCols.contains(fieldName))) {
                fields.add(fieldName);
            }
        }
        return fields;
    }
}
