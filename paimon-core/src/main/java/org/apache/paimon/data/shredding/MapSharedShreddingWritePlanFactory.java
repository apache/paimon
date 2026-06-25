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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatMetadataUtils;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.SupportsWriterMetadata;
import org.apache.paimon.io.FileMetadataFinalizer;
import org.apache.paimon.io.RowDataFileWritePlan;
import org.apache.paimon.io.RowDataFileWritePlanFactory;
import org.apache.paimon.io.RowDataTransform;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Creates per-file write plans for MAP shared-shredding row data files. */
public class MapSharedShreddingWritePlanFactory implements RowDataFileWritePlanFactory {

    private final RowType logicalType;
    private final MapSharedShreddingContext context;
    @Nullable private final String fieldIdKey;

    public MapSharedShreddingWritePlanFactory(
            RowType logicalType, MapSharedShreddingContext context, @Nullable String fieldIdKey) {
        this.logicalType = logicalType;
        this.context = context;
        this.fieldIdKey = fieldIdKey;
    }

    @Override
    public RowDataFileWritePlan create() {
        MapSharedShreddingRowConverter converter =
                new MapSharedShreddingRowConverter(logicalType, context.computeNextK());
        return new SharedShreddingWritePlan(converter);
    }

    private class SharedShreddingWritePlan implements RowDataFileWritePlan {

        private final MapSharedShreddingRowConverter converter;
        private final RowDataTransform rowTransform;
        private final FileMetadataFinalizer metadataFinalizer;

        private SharedShreddingWritePlan(MapSharedShreddingRowConverter converter) {
            this.converter = converter;
            this.rowTransform =
                    new RowDataTransform() {
                        @Override
                        public RowType physicalType() {
                            return converter.physicalType();
                        }

                        @Override
                        public InternalRow transform(InternalRow row) {
                            return converter.convert(row);
                        }
                    };
            this.metadataFinalizer = new SharedShreddingMetadataFinalizer(converter);
        }

        @Override
        public RowType physicalType() {
            return converter.physicalType();
        }

        @Nullable
        @Override
        public RowDataTransform rowTransform() {
            return rowTransform;
        }

        @Nullable
        @Override
        public FileMetadataFinalizer metadataFinalizer() {
            return metadataFinalizer;
        }
    }

    private class SharedShreddingMetadataFinalizer implements FileMetadataFinalizer {

        private final MapSharedShreddingRowConverter converter;

        private SharedShreddingMetadataFinalizer(MapSharedShreddingRowConverter converter) {
            this.converter = converter;
        }

        @Override
        public void beforeClose(FormatWriter writer) throws IOException {
            if (!(writer instanceof SupportsWriterMetadata)) {
                throw new UnsupportedOperationException(
                        "MAP shared-shredding requires a format writer supporting metadata.");
            }

            Map<String, Map<String, String>> fieldMetadata = new LinkedHashMap<>();
            for (String fieldName : converter.shreddingFieldNames()) {
                MapSharedShreddingFieldMeta fieldMeta = converter.buildFieldMeta(fieldName);
                Map<String, String> metadata = new LinkedHashMap<>();
                MapSharedShreddingUtils.serializeMetadata(
                        fieldMeta, MapSharedShreddingDefine.DEFAULT_DICT_COMPRESSION, metadata);
                fieldMetadata.put(fieldName, metadata);
                context.reportFileStats(fieldName, fieldMeta.maxRowWidth());
            }

            ((SupportsWriterMetadata) writer)
                    .addMetadata(
                            Collections.singletonMap(
                                    FormatMetadataUtils.ARROW_SCHEMA_METADATA_KEY,
                                    FormatMetadataUtils.buildArrowSchemaMetadata(
                                            converter.physicalType(), fieldMetadata, fieldIdKey)));
        }
    }
}
