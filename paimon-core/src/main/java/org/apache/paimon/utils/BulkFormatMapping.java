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

package org.apache.paimon.utils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/** Class with index mapping and bulk format. */
public class BulkFormatMapping {

    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castMapping;
    @Nullable private final Pair<int[], RowType> partitionPair;
    private final FormatReaderFactory bulkFormat;

    public BulkFormatMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory bulkFormat) {
        this.indexMapping = indexMapping;
        this.castMapping = castMapping;
        this.bulkFormat = bulkFormat;
        this.partitionPair = partitionPair;
    }

    @Nullable
    public int[] getIndexMapping() {
        return indexMapping;
    }

    @Nullable
    public CastFieldGetter[] getCastMapping() {
        return castMapping;
    }

    @Nullable
    public Pair<int[], RowType> getPartitionPair() {
        return partitionPair;
    }

    public FormatReaderFactory getReaderFactory() {
        return bulkFormat;
    }

    public static BulkFormatMappingBuilder newBuilder(
            FileFormatDiscover formatDiscover,
            KeyValueFieldsExtractor extractor,
            int[][] keyProjection,
            int[][] valueProjection,
            @Nullable List<Predicate> filters) {
        return new BulkFormatMappingBuilder(
                formatDiscover, extractor, keyProjection, valueProjection, filters);
    }

    /** Builder to build {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {

        private final FileFormatDiscover formatDiscover;
        private final KeyValueFieldsExtractor extractor;
        private final int[][] keyProjection;
        private final int[][] valueProjection;
        @Nullable private final List<Predicate> filters;

        private BulkFormatMappingBuilder(
                FileFormatDiscover formatDiscover,
                KeyValueFieldsExtractor extractor,
                int[][] keyProjection,
                int[][] valueProjection,
                @Nullable List<Predicate> filters) {
            this.formatDiscover = formatDiscover;
            this.extractor = extractor;
            this.keyProjection = keyProjection;
            this.valueProjection = valueProjection;
            this.filters = filters;
        }

        public BulkFormatMapping build(
                String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {
            List<DataField> tableKeyFields = extractor.keyFields(tableSchema);
            List<DataField> tableValueFields = extractor.valueFields(tableSchema);
            int[][] tableProjection =
                    KeyValue.project(keyProjection, valueProjection, tableKeyFields.size());

            List<DataField> dataKeyFields = extractor.keyFields(dataSchema);
            List<DataField> dataValueFields = extractor.valueFields(dataSchema);

            RowType keyType = new RowType(dataKeyFields);
            RowType valueType = new RowType(dataValueFields);
            RowType dataRecordType = KeyValue.schema(keyType, valueType);

            int[][] dataKeyProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableKeyFields, dataKeyFields, keyProjection);
            int[][] dataValueProjection =
                    SchemaEvolutionUtil.createDataProjection(
                            tableValueFields, dataValueFields, valueProjection);
            int[][] dataProjection =
                    KeyValue.project(dataKeyProjection, dataValueProjection, dataKeyFields.size());

            /*
             * We need to create index mapping on projection instead of key and value separately
             * here, for example
             *
             * <ul>
             *   <li>the table key fields: 1->d, 3->a, 4->b, 5->c
             *   <li>the data key fields: 1->a, 2->b, 3->c
             * </ul>
             *
             * <p>The value fields of table and data are 0->value_count, the key and value
             * projections are as follows
             *
             * <ul>
             *   <li>table key projection: [0, 1, 2, 3], value projection: [0], data projection: [0,
             *       1, 2, 3, 4, 5, 6] which 4/5 is seq/kind and 6 is value
             *   <li>data key projection: [0, 1, 2], value projection: [0], data projection: [0, 1,
             *       2, 3, 4, 5] where 3/4 is seq/kind and 5 is value
             * </ul>
             *
             * <p>We will get value index mapping null from above and we can't create projection
             * index mapping based on key and value index mapping any more.
             */
            IndexCastMapping indexCastMapping =
                    SchemaEvolutionUtil.createIndexCastMapping(
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

            Pair<int[], RowType> partitionPair = null;
            if (!dataSchema.partitionKeys().isEmpty()) {
                Pair<int[], int[][]> partitionMapping =
                        PartitionUtils.constructPartitionMapping(
                                dataRecordType, dataSchema.partitionKeys(), dataProjection);
                // is partition fields are not selected, we just do nothing.
                if (partitionMapping != null) {
                    dataProjection = partitionMapping.getRight();
                    partitionPair =
                            Pair.of(
                                    partitionMapping.getLeft(),
                                    dataSchema.projectedLogicalRowType(dataSchema.partitionKeys()));
                }
            }
            RowType projectedRowType = Projection.of(dataProjection).project(dataRecordType);
            return new BulkFormatMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    partitionPair,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(projectedRowType, dataFilters));
        }
    }
}
