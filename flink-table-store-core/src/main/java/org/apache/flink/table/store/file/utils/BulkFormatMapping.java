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
            @Nullable List<Predicate> filters) {
        return new BulkFormatMappingBuilder(
                fileFormat, extractor, keyProjection, valueProjection, filters);
    }

    /** Builder to build {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {
        private final FileFormat fileFormat;
        private final KeyValueFieldsExtractor extractor;
        private final int[][] keyProjection;
        private final int[][] valueProjection;
        @Nullable private final List<Predicate> filters;

        private BulkFormatMappingBuilder(
                FileFormat fileFormat,
                KeyValueFieldsExtractor extractor,
                int[][] keyProjection,
                int[][] valueProjection,
                @Nullable List<Predicate> filters) {
            this.fileFormat = fileFormat;
            this.extractor = extractor;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.filters = filters;
        }

        public BulkFormatMapping build(TableSchema tableSchema, TableSchema dataSchema) {
            List<DataField> tableKeyFields = extractor.keyFields(tableSchema);
            List<DataField> tableValueFields = extractor.valueFields(tableSchema);
            int[][] tableProjection =
                    KeyValue.project(keyProjection, valueProjection, tableKeyFields.size());

            List<DataField> dataKeyFields = extractor.keyFields(dataSchema);
            List<DataField> dataValueFields = extractor.valueFields(dataSchema);

            RowType keyType = RowDataType.toRowType(false, dataKeyFields);
            RowType valueType = RowDataType.toRowType(false, dataValueFields);
            RowType dataRecordType = KeyValue.schema(keyType, valueType);

            int[][] dataKeyProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableKeyFields, dataKeyFields, keyProjection);
            int[][] dataValueProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableValueFields, dataValueFields, valueProjection);
            int[][] dataProjection =
                    KeyValue.project(dataKeyProjection, dataValueProjection, dataKeyFields.size());

            /**
             * We need to create index mapping on projection instead of key and value separately
             * here, for example
             *
             * <ul>
             *   <li>the table key fields: 1->d, 3->a, 4->b, 5->c
             *   <li>the data key fields: 1->a, 2->b, 3->c
             * </ul>
             *
             * The value fields of table and data are 0->value_count, the key and value projections
             * are as follows
             *
             * <ul>
             *   <li>table key projection: [0, 1, 2, 3], value projection: [0], data projection: [0,
             *       1, 2, 3, 4, 5, 6] which 4/5 is seq/kind and 6 is value
             *   <li>data key projection: [0, 1, 2], value projection: [0], data projection: [0, 1,
             *       2, 3, 4, 5] where 3/4 is seq/kind and 5 is value
             * </ul>
             *
             * We will get value index mapping null fro above and we can't create projection index
             * mapping based on key and value index mapping any more.
             */
            int[] indexMapping =
                    SchemaEvolutionUtil.createIndexMapping(
                            Projection.of(tableProjection).toTopLevelIndexes(),
                            tableKeyFields,
                            tableValueFields,
                            Projection.of(dataProjection).toTopLevelIndexes(),
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
