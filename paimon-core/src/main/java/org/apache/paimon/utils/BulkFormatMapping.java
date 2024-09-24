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

import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.IndexCastMapping;
import org.apache.paimon.schema.SchemaEvolutionUtil;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Class with index mapping and bulk format. */
public class BulkFormatMapping {

    @Nullable private final int[] indexMapping;
    @Nullable private final CastFieldGetter[] castMapping;
    @Nullable private final Pair<int[], RowType> partitionPair;
    private final FormatReaderFactory bulkFormat;
    private final TableSchema dataSchema;
    private final List<Predicate> dataFilters;

    public BulkFormatMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory bulkFormat,
            TableSchema dataSchema,
            List<Predicate> dataFilters) {
        this.indexMapping = indexMapping;
        this.castMapping = castMapping;
        this.bulkFormat = bulkFormat;
        this.partitionPair = partitionPair;
        this.dataSchema = dataSchema;
        this.dataFilters = dataFilters;
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

    public TableSchema getDataSchema() {
        return dataSchema;
    }

    public List<Predicate> getDataFilters() {
        return dataFilters;
    }

    /** Builder for {@link BulkFormatMapping}. */
    public static class BulkFormatMappingBuilder {

        private final FileFormatDiscover formatDiscover;
        private final List<DataField> readTableFields;
        private final Function<TableSchema, List<DataField>> fieldsExtractor;
        @Nullable private final List<Predicate> filters;

        public BulkFormatMappingBuilder(
                FileFormatDiscover formatDiscover,
                List<DataField> readTableFields,
                Function<TableSchema, List<DataField>> fieldsExtractor,
                @Nullable List<Predicate> filters) {
            this.formatDiscover = formatDiscover;
            this.readTableFields = readTableFields;
            this.fieldsExtractor = fieldsExtractor;
            this.filters = filters;
        }

        public BulkFormatMapping build(
                String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {

            List<DataField> readDataFields = readDataFields(dataSchema);

            // build index cast mapping
            IndexCastMapping indexCastMapping =
                    SchemaEvolutionUtil.createIndexCastMapping(readTableFields, readDataFields);

            // build partition mapping and filter partition fields
            Pair<Pair<int[], RowType>, List<DataField>>
                    partitionMappingAndFieldsWithoutPartitionPair =
                            PartitionUtils.constructPartitionMapping(dataSchema, readDataFields);
            Pair<int[], RowType> partitionMapping =
                    partitionMappingAndFieldsWithoutPartitionPair.getLeft();

            // build read row type
            RowType readDataRowType =
                    new RowType(partitionMappingAndFieldsWithoutPartitionPair.getRight());

            // build read filters
            List<Predicate> readFilters = readFilters(filters, tableSchema, dataSchema);

            return new BulkFormatMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    partitionMapping,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(readDataRowType, readFilters),
                    dataSchema,
                    readFilters);
        }

        private List<DataField> readDataFields(TableSchema dataSchema) {
            List<DataField> dataFields = fieldsExtractor.apply(dataSchema);
            List<DataField> readDataFields = new ArrayList<>();
            for (DataField dataField : dataFields) {
                readTableFields.stream()
                        .filter(f -> f.id() == dataField.id())
                        .findFirst()
                        .ifPresent(
                                f -> {
                                    if (f.type() instanceof RowType) {
                                        RowType tableFieldType = (RowType) f.type();
                                        RowType dataFieldType = (RowType) dataField.type();
                                        checkState(tableFieldType.prunedFrom(dataFieldType));
                                        // Since the nested type schema evolution is not supported,
                                        // directly copy the fields from tableField's type to
                                        // dataField's type.
                                        // todo: support nested type schema evolutions.
                                        readDataFields.add(
                                                dataField.newType(
                                                        dataFieldType.copy(
                                                                tableFieldType.getFields())));
                                    } else {
                                        readDataFields.add(dataField);
                                    }
                                });
            }
            return readDataFields;
        }

        private List<Predicate> readFilters(
                List<Predicate> filters, TableSchema tableSchema, TableSchema dataSchema) {
            List<Predicate> dataFilters =
                    tableSchema.id() == dataSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.createDataFilters(
                                    tableSchema.fields(), dataSchema.fields(), filters);

            // Skip pushing down partition filters to reader.
            return excludePredicateWithFields(
                    dataFilters, new HashSet<>(dataSchema.partitionKeys()));
        }
    }
}
