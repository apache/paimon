/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.file.utils;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.file.schema.KeyValueFieldsExtractor;
import org.apache.flink.table.store.file.schema.RowDataType;
import org.apache.flink.table.store.file.schema.SchemaEvolutionUtil;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** Class with index mapping and bulk format. */
public class BulkFormatMapping {
    @Nullable private final int[] indexMapping;
    private final BulkFormat<RowData, FileSourceSplit> bulkFormat;

    public BulkFormatMapping(int[] indexMapping, BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        this.indexMapping = indexMapping;
        this.bulkFormat = bulkFormat;
    }

    @Nullable
    public int[] getIndexMapping() {
        return indexMapping;
    }

    public BulkFormat<RowData, FileSourceSplit> getReaderFactory() {
        return bulkFormat;
    }

    public static BulkFormatMappingBuilder newBuilder(
            FileFormat fileFormat,
            KeyValueFieldsExtractor extractor,
            int[][] keyProjection,
            int[][] valueProjection,
            int[][] projection,
            @Nullable List<Predicate> filters) {
        return new BulkFormatMappingBuilder(
                fileFormat, extractor, keyProjection, valueProjection, projection, filters);
    }

    /** Builder to build {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {
        private final FileFormat fileFormat;
        private final KeyValueFieldsExtractor extractor;
        private final int[][] keyProjection;
        private final int[][] valueProjection;
        private final int[][] projection;
        @Nullable private final List<Predicate> filters;

        private BulkFormatMappingBuilder(
                FileFormat fileFormat,
                KeyValueFieldsExtractor extractor,
                int[][] keyProjection,
                int[][] valueProjection,
                int[][] projection,
                @Nullable List<Predicate> filters) {
            this.fileFormat = fileFormat;
            this.extractor = extractor;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.projection = projection;
            this.filters = filters;
        }

        public BulkFormatMapping build(TableSchema tableSchema, TableSchema dataSchema) {
            List<DataField> tableKeyFields = extractor.keyFields(tableSchema);
            List<DataField> tableValueFields = extractor.valueFields(tableSchema);

            List<DataField> dataKeyFields = extractor.keyFields(dataSchema);
            List<DataField> dataValueFields = extractor.valueFields(dataSchema);
            int[][] dataKeyProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableKeyFields, dataKeyFields, keyProjection);
            int[][] dataValueProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableValueFields, dataValueFields, valueProjection);

            RowType keyType = RowDataType.toRowType(false, dataKeyFields);
            RowType valueType = RowDataType.toRowType(false, dataValueFields);
            RowType dataRecordType = KeyValue.schema(keyType, valueType);
            int[][] dataProjection =
                    KeyValue.project(
                            dataKeyProjection, dataValueProjection, keyType.getFieldCount());

            int[] indexMapping =
                    SchemaEvolutionUtil.createIndexMapping(
                            Projection.of(projection).toTopLevelIndexes(),
                            keyProjection.length,
                            tableKeyFields,
                            tableValueFields,
                            Projection.of(dataProjection).toTopLevelIndexes(),
                            dataKeyProjection.length,
                            dataKeyFields,
                            dataValueFields);
            List<Predicate> dataFilters =
                    tableSchema.id() == dataSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.createDataFilters(
                                    tableSchema.fields(), dataSchema.fields(), filters);
            return new BulkFormatMapping(
                    indexMapping,
                    fileFormat.createReaderFactory(dataRecordType, dataProjection, dataFilters));
        }
    }
}
