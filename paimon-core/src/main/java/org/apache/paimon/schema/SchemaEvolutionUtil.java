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

package org.apache.paimon.schema;

import org.apache.paimon.casting.CastElementGetter;
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedArray;
import org.apache.paimon.casting.CastedMap;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateReplaceVisitor;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Utils for schema evolution. */
public class SchemaEvolutionUtil {

    private static final int NULL_FIELD_INDEX = -1;

    /**
     * Create index mapping from table fields to underlying data fields. For example, the table and
     * data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 6->b, 3->a
     *   <li>data fields: 1->a, 3->c
     * </ul>
     *
     * <p>We can get the index mapping [0, -1, 1], in which 0 is the index of table field 1->c in
     * data fields, -1 is the index of 6->b in data fields and 1 is the index of 3->a in data
     * fields.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @return the index mapping
     */
    @Nullable
    public static int[] createIndexMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = new int[tableFields.size()];
        Map<Integer, Integer> fieldIdToIndex = new HashMap<>();
        for (int i = 0; i < dataFields.size(); i++) {
            fieldIdToIndex.put(dataFields.get(i).id(), i);
        }

        for (int i = 0; i < tableFields.size(); i++) {
            int fieldId = tableFields.get(i).id();
            Integer dataFieldIndex = fieldIdToIndex.get(fieldId);
            if (dataFieldIndex != null) {
                indexMapping[i] = dataFieldIndex;
            } else {
                indexMapping[i] = NULL_FIELD_INDEX;
            }
        }

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] != i) {
                return indexMapping;
            }
        }
        return null;
    }

    /** Create index mapping from table fields to underlying data fields. */
    public static IndexCastMapping createIndexCastMapping(
            List<DataField> tableFields, List<DataField> dataFields) {
        int[] indexMapping = createIndexMapping(tableFields, dataFields);
        CastFieldGetter[] castMapping =
                createCastFieldGetterMapping(tableFields, dataFields, indexMapping);
        return new IndexCastMapping() {
            @Nullable
            @Override
            public int[] getIndexMapping() {
                return indexMapping;
            }

            @Nullable
            @Override
            public CastFieldGetter[] getCastMapping() {
                return castMapping;
            }
        };
    }

    /**
     * When pushing down filters after schema evolution, we should devolve the literals from new
     * types (in dataFields) to original types (in tableFields). We will visit all predicate in
     * filters, reset its field index, name and type, and ignore predicate if the field is not
     * exist.
     *
     * @param tableFields the table fields
     * @param dataFields the underlying data fields
     * @param filters the filters
     * @return the data filters
     */
    @Nullable
    public static List<Predicate> devolveDataFilters(
            List<DataField> tableFields, List<DataField> dataFields, List<Predicate> filters) {
        if (filters == null) {
            return null;
        }

        Map<String, DataField> nameToTableFields =
                tableFields.stream().collect(Collectors.toMap(DataField::name, f -> f));
        LinkedHashMap<Integer, DataField> idToDataFields = new LinkedHashMap<>();
        dataFields.forEach(f -> idToDataFields.put(f.id(), f));
        List<Predicate> dataFilters = new ArrayList<>(filters.size());

        PredicateReplaceVisitor visitor =
                predicate -> {
                    DataField tableField =
                            checkNotNull(
                                    nameToTableFields.get(predicate.fieldName()),
                                    String.format("Find no field %s", predicate.fieldName()));
                    DataField dataField = idToDataFields.get(tableField.id());
                    if (dataField == null) {
                        return Optional.empty();
                    }

                    return CastExecutors.castLiteralsWithEvolution(
                                    predicate.literals(), predicate.type(), dataField.type())
                            .map(
                                    literals ->
                                            new LeafPredicate(
                                                    predicate.function(),
                                                    dataField.type(),
                                                    indexOf(dataField, idToDataFields),
                                                    dataField.name(),
                                                    literals));
                };

        for (Predicate predicate : filters) {
            predicate.visit(visitor).ifPresent(dataFilters::add);
        }
        return dataFilters;
    }

    private static int indexOf(DataField dataField, LinkedHashMap<Integer, DataField> dataFields) {
        int index = 0;
        for (Map.Entry<Integer, DataField> entry : dataFields.entrySet()) {
            if (dataField.id() == entry.getKey()) {
                return index;
            }
            index++;
        }

        throw new IllegalArgumentException(
                String.format("Can't find data field %s", dataField.name()));
    }

    /**
     * Create getter and casting mapping from table fields to underlying data fields with given
     * index mapping. For example, the table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c INT, 6->b STRING, 3->a BIGINT
     *   <li>data fields: 1->a BIGINT, 3->c DOUBLE
     * </ul>
     *
     * <p>We can get the column types (1->a BIGINT), (3->c DOUBLE) from data fields for (1->c INT)
     * and (3->a BIGINT) in table fields through index mapping [0, -1, 1], then compare the data
     * type and create getter and casting mapping.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param indexMapping the index mapping from table fields to data fields
     * @return the getter and casting mapping
     */
    private static CastFieldGetter[] createCastFieldGetterMapping(
            List<DataField> tableFields, List<DataField> dataFields, int[] indexMapping) {
        CastFieldGetter[] converterMapping = new CastFieldGetter[tableFields.size()];
        boolean castExist = false;

        for (int i = 0; i < tableFields.size(); i++) {
            int dataIndex = indexMapping == null ? i : indexMapping[i];
            if (dataIndex < 0) {
                converterMapping[i] =
                        new CastFieldGetter(row -> null, CastExecutors.identityCastExecutor());
            } else {
                DataField tableField = tableFields.get(i);
                DataField dataField = dataFields.get(dataIndex);
                if (!dataField.type().equalsIgnoreNullable(tableField.type())) {
                    castExist = true;
                }

                // Create getter with index i and projected row data will convert to underlying data
                converterMapping[i] =
                        new CastFieldGetter(
                                InternalRowUtils.createNullCheckingFieldGetter(dataField.type(), i),
                                createCastExecutor(dataField.type(), tableField.type()));
            }
        }

        return castExist ? converterMapping : null;
    }

    private static CastExecutor<?, ?> createCastExecutor(DataType inputType, DataType targetType) {
        if (targetType.equalsIgnoreNullable(inputType)) {
            return CastExecutors.identityCastExecutor();
        } else if (inputType instanceof RowType && targetType instanceof RowType) {
            return createRowCastExecutor((RowType) inputType, (RowType) targetType);
        } else if (inputType instanceof ArrayType && targetType instanceof ArrayType) {
            return createArrayCastExecutor((ArrayType) inputType, (ArrayType) targetType);
        } else if (inputType instanceof MapType && targetType instanceof MapType) {
            return createMapCastExecutor((MapType) inputType, (MapType) targetType);
        } else {
            return checkNotNull(
                    CastExecutors.resolve(inputType, targetType),
                    "Cannot cast from type %s to type %s",
                    inputType,
                    targetType);
        }
    }

    private static CastExecutor<InternalRow, InternalRow> createRowCastExecutor(
            RowType inputType, RowType targetType) {
        int[] indexMapping = createIndexMapping(targetType.getFields(), inputType.getFields());
        CastFieldGetter[] castFieldGetters =
                createCastFieldGetterMapping(
                        targetType.getFields(), inputType.getFields(), indexMapping);

        ProjectedRow projectedRow = indexMapping == null ? null : ProjectedRow.from(indexMapping);
        CastedRow castedRow = castFieldGetters == null ? null : CastedRow.from(castFieldGetters);
        return value -> {
            if (projectedRow != null) {
                value = projectedRow.replaceRow(value);
            }
            if (castedRow != null) {
                value = castedRow.replaceRow(value);
            }
            return value;
        };
    }

    private static CastExecutor<InternalArray, InternalArray> createArrayCastExecutor(
            ArrayType inputType, ArrayType targetType) {
        CastElementGetter castElementGetter =
                new CastElementGetter(
                        InternalArray.createElementGetter(inputType.getElementType()),
                        createCastExecutor(
                                inputType.getElementType(), targetType.getElementType()));

        CastedArray castedArray = CastedArray.from(castElementGetter);
        return castedArray::replaceArray;
    }

    private static CastExecutor<InternalMap, InternalMap> createMapCastExecutor(
            MapType inputType, MapType targetType) {
        checkState(
                inputType.getKeyType().equals(targetType.getKeyType()),
                "Cannot cast map type %s to map type %s, because they have different key types.",
                inputType.getKeyType(),
                targetType.getKeyType());
        CastElementGetter castElementGetter =
                new CastElementGetter(
                        InternalArray.createElementGetter(inputType.getValueType()),
                        createCastExecutor(inputType.getValueType(), targetType.getValueType()));

        CastedMap castedMap = CastedMap.from(castElementGetter);
        return castedMap::replaceMap;
    }
}
