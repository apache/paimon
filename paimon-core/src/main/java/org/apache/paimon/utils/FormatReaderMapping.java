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
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.predicate.PredicateBuilder.excludePredicateWithFields;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_ID_START;

/** Class with index mapping and format reader. */
public class FormatReaderMapping {

    // Index mapping from data schema fields to table schema fields, this is used to realize paimon
    // schema evolution. And it combines trimeedKeyMapping, which maps key fields to the value
    // fields
    @Nullable private final int[] indexMapping;
    // help indexMapping to cast different data type
    @Nullable private final CastFieldGetter[] castMapping;
    // partition fields mapping, add partition fields to the read fields
    @Nullable private final Pair<int[], RowType> partitionPair;
    private final FormatReaderFactory readerFactory;
    private final TableSchema dataSchema;
    private final List<Predicate> dataFilters;

    public FormatReaderMapping(
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable int[] trimmedKeyMapping,
            @Nullable Pair<int[], RowType> partitionPair,
            FormatReaderFactory readerFactory,
            TableSchema dataSchema,
            List<Predicate> dataFilters) {
        this.indexMapping = combine(indexMapping, trimmedKeyMapping);
        this.castMapping = castMapping;
        this.readerFactory = readerFactory;
        this.partitionPair = partitionPair;
        this.dataSchema = dataSchema;
        this.dataFilters = dataFilters;
    }

    private int[] combine(@Nullable int[] indexMapping, @Nullable int[] trimmedKeyMapping) {
        if (indexMapping == null) {
            return trimmedKeyMapping;
        }
        if (trimmedKeyMapping == null) {
            return indexMapping;
        }

        int[] combined = new int[indexMapping.length];

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] < 0) {
                combined[i] = indexMapping[i];
            } else {
                combined[i] = trimmedKeyMapping[indexMapping[i]];
            }
        }
        return combined;
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
        return readerFactory;
    }

    public TableSchema getDataSchema() {
        return dataSchema;
    }

    public List<Predicate> getDataFilters() {
        return dataFilters;
    }

    /** Builder for {@link FormatReaderMapping}. */
    public static class Builder {

        private final FileFormatDiscover formatDiscover;
        private final List<DataField> readTableFields;
        private final Function<TableSchema, List<DataField>> fieldsExtractor;
        @Nullable private final List<Predicate> filters;

        public Builder(
                FileFormatDiscover formatDiscover,
                List<DataField> readTableFields,
                Function<TableSchema, List<DataField>> fieldsExtractor,
                @Nullable List<Predicate> filters) {
            this.formatDiscover = formatDiscover;
            this.readTableFields = readTableFields;
            this.fieldsExtractor = fieldsExtractor;
            this.filters = filters;
        }

        /**
         * There are three steps here to build {@link FormatReaderMapping}:
         *
         * <p>1. Calculate the readDataFields, which is what we intend to read from the data schema.
         * Meanwhile, generate the indexCastMapping, which is used to map the index of the
         * readDataFields to the index of the data schema.
         *
         * <p>2. Calculate the mapping to trim _KEY_ fields. For example: we want _KEY_a, _KEY_b,
         * _FIELD_SEQUENCE, _ROW_KIND, a, b, c, d, e, f, g from the data, but actually we don't need
         * to read _KEY_a and a, _KEY_b and b the same time, so we need to trim them. So we mapping
         * it: read before: _KEY_a, _KEY_b, _FIELD_SEQUENCE, _ROW_KIND, a, b, c, d, e, f, g read
         * after: a, b, _FIELD_SEQUENCE, _ROW_KIND, c, d, e, f, g and the mapping is
         * [0,1,2,3,0,1,4,5,6,7,8], it converts the [read after] columns to [read before] columns.
         *
         * <p>3. We want read much fewer fields than readDataFields, so we kick out the partition
         * fields. We generate the partitionMappingAndFieldsWithoutPartitionPair which helps reduce
         * the real read fields and tell us how to map it back.
         */
        public FormatReaderMapping build(
                String formatIdentifier, TableSchema tableSchema, TableSchema dataSchema) {

            // extract the whole data fields in logic.
            List<DataField> allDataFields = fieldsExtractor.apply(dataSchema);
            List<DataField> readDataFields = readDataFields(allDataFields);
            // build index cast mapping
            IndexCastMapping indexCastMapping =
                    SchemaEvolutionUtil.createIndexCastMapping(readTableFields, readDataFields);

            // map from key fields reading to value fields reading
            Pair<int[], RowType> trimmedKeyPair = trimKeyFields(readDataFields, allDataFields);

            // build partition mapping and filter partition fields
            Pair<Pair<int[], RowType>, List<DataField>>
                    partitionMappingAndFieldsWithoutPartitionPair =
                            PartitionUtils.constructPartitionMapping(
                                    dataSchema, trimmedKeyPair.getRight().getFields());
            Pair<int[], RowType> partitionMapping =
                    partitionMappingAndFieldsWithoutPartitionPair.getLeft();

            RowType readRowType =
                    new RowType(partitionMappingAndFieldsWithoutPartitionPair.getRight());

            // build read filters
            List<Predicate> readFilters = readFilters(filters, tableSchema, dataSchema);

            return new FormatReaderMapping(
                    indexCastMapping.getIndexMapping(),
                    indexCastMapping.getCastMapping(),
                    trimmedKeyPair.getLeft(),
                    partitionMapping,
                    formatDiscover
                            .discover(formatIdentifier)
                            .createReaderFactory(readRowType, readFilters),
                    dataSchema,
                    readFilters);
        }

        static Pair<int[], RowType> trimKeyFields(
                List<DataField> fieldsWithoutPartition, List<DataField> fields) {
            int[] map = new int[fieldsWithoutPartition.size()];
            List<DataField> trimmedFields = new ArrayList<>();
            Map<Integer, DataField> fieldMap = new HashMap<>();
            Map<Integer, Integer> positionMap = new HashMap<>();

            for (DataField field : fields) {
                fieldMap.put(field.id(), field);
            }

            for (int i = 0; i < fieldsWithoutPartition.size(); i++) {
                DataField field = fieldsWithoutPartition.get(i);
                boolean keyField = SpecialFields.isKeyField(field.name());
                int id = keyField ? field.id() - KEY_FIELD_ID_START : field.id();
                // field in data schema
                DataField f = fieldMap.get(id);

                if (f != null) {
                    if (positionMap.containsKey(id)) {
                        map[i] = positionMap.get(id);
                    } else {
                        map[i] = positionMap.computeIfAbsent(id, k -> trimmedFields.size());
                        // If the target field is not key field, we remain what it is, because it
                        // may be projected. Example: the target field is a row type, but only read
                        // the few fields in it. If we simply trimmedFields.add(f), we will read
                        // more fields than we need.
                        trimmedFields.add(keyField ? f : field);
                    }
                } else {
                    throw new RuntimeException("Can't find field with id: " + id + " in fields.");
                }
            }

            return Pair.of(map, new RowType(trimmedFields));
        }

        private List<DataField> readDataFields(List<DataField> allDataFields) {
            List<DataField> readDataFields = new ArrayList<>();
            for (DataField dataField : allDataFields) {
                readTableFields.stream()
                        .filter(f -> f.id() == dataField.id())
                        .findFirst()
                        .ifPresent(
                                field -> {
                                    DataType prunedType =
                                            pruneDataType(field.type(), dataField.type());
                                    if (prunedType != null) {
                                        readDataFields.add(dataField.newType(prunedType));
                                    }
                                });
            }
            return readDataFields;
        }

        @Nullable
        private DataType pruneDataType(DataType readType, DataType dataType) {
            switch (readType.getTypeRoot()) {
                case ROW:
                    RowType r = (RowType) readType;
                    RowType d = (RowType) dataType;
                    ArrayList<DataField> newFields = new ArrayList<>();
                    for (DataField rf : r.getFields()) {
                        if (d.containsField(rf.id())) {
                            DataField df = d.getField(rf.id());
                            DataType newType = pruneDataType(rf.type(), df.type());
                            if (newType == null) {
                                continue;
                            }
                            newFields.add(df.newType(newType));
                        }
                    }
                    if (newFields.isEmpty()) {
                        // When all fields are pruned, we should not return an empty row type
                        return null;
                    }
                    return d.copy(newFields);
                case MAP:
                    DataType keyType =
                            pruneDataType(
                                    ((MapType) readType).getKeyType(),
                                    ((MapType) dataType).getKeyType());
                    DataType valueType =
                            pruneDataType(
                                    ((MapType) readType).getValueType(),
                                    ((MapType) dataType).getValueType());
                    if (keyType == null || valueType == null) {
                        return null;
                    }
                    return ((MapType) dataType).newKeyValueType(keyType, valueType);
                case ARRAY:
                    DataType elementType =
                            pruneDataType(
                                    ((ArrayType) readType).getElementType(),
                                    ((ArrayType) dataType).getElementType());
                    if (elementType == null) {
                        return null;
                    }
                    return ((ArrayType) dataType).newElementType(elementType);
                default:
                    return dataType;
            }
        }

        private List<Predicate> readFilters(
                List<Predicate> filters, TableSchema tableSchema, TableSchema dataSchema) {
            List<Predicate> dataFilters =
                    tableSchema.id() == dataSchema.id()
                            ? filters
                            : SchemaEvolutionUtil.devolveDataFilters(
                                    tableSchema.fields(), dataSchema.fields(), filters);

            // Skip pushing down partition filters to reader.
            return excludePredicateWithFields(
                    dataFilters, new HashSet<>(dataSchema.partitionKeys()));
        }
    }
}
