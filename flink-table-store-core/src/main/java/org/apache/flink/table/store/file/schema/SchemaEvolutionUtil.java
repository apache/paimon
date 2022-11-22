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

package org.apache.flink.table.store.file.schema;

import org.apache.flink.table.store.file.predicate.CompoundPredicate;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.Predicate;

import org.apache.flink.shaded.guava30.com.google.common.primitives.Ints;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for schema evolution. */
public class SchemaEvolutionUtil {

    private static final int NULL_FIELD_INDEX = -1;

    /**
     * Create index mapping from table fields to underlying data fields.
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

    /**
     * Create index mapping from table projection to underlying data projection.
     *
     * @param tableProjection the table projection
     * @param tableFields the fields in table
     * @param dataProjection the underlying data projection
     * @param dataFields the fields in underlying data
     * @return the index mapping
     */
    @Nullable
    public static int[] createIndexMapping(
            int[] tableProjection,
            List<DataField> tableFields,
            int[] dataProjection,
            List<DataField> dataFields) {
        return createIndexMapping(
                tableProjection,
                tableProjection.length,
                tableFields,
                Collections.emptyList(),
                dataProjection,
                dataProjection.length,
                dataFields,
                Collections.emptyList());
    }

    /**
     * Create index mapping from table projection to underlying data projection for key value.
     *
     * @param tableProjection the table projection
     * @param tableKeyCount the key count in table
     * @param tableKeyFields the key fields in table
     * @param tableValueFields the value fields in table
     * @param dataProjection the data projection
     * @param dataKeyCount the data key count
     * @param dataKeyFields the fields in underlying data
     * @param dataValueFields the fields in underlying data
     * @return the index mapping
     */
    @Nullable
    public static int[] createIndexMapping(
            int[] tableProjection,
            int tableKeyCount,
            List<DataField> tableKeyFields,
            List<DataField> tableValueFields,
            int[] dataProjection,
            int dataKeyCount,
            List<DataField> dataKeyFields,
            List<DataField> dataValueFields) {
        List<Integer> tableKeyFieldIdList =
                tableKeyFields.stream().map(DataField::id).collect(Collectors.toList());
        List<Integer> dataKeyFieldIdList =
                dataKeyFields.stream().map(DataField::id).collect(Collectors.toList());
        int[] indexMapping = new int[tableProjection.length];

        int[] dataKeyProjection = Arrays.copyOf(dataProjection, dataKeyCount);
        for (int i = 0; i < tableKeyCount; i++) {
            int fieldId = tableKeyFieldIdList.get(tableProjection[i]);
            int dataFieldIndex = dataKeyFieldIdList.indexOf(fieldId);
            indexMapping[i] = Ints.indexOf(dataKeyProjection, dataFieldIndex);
        }
        if (tableProjection.length > tableKeyCount + 2) {
            // seq and value kind
            for (int i = tableKeyCount; i < tableKeyCount + 2; i++) {
                indexMapping[i] = i + dataKeyCount - tableKeyCount;
            }

            int[] dataValueProjection =
                    Arrays.copyOfRange(dataProjection, dataKeyCount + 2, dataProjection.length);
            for (int i = 0; i < dataValueProjection.length; i++) {
                dataValueProjection[i] = dataValueProjection[i] - dataKeyFields.size() - 2;
            }
            List<Integer> tableValueFieldIdList =
                    tableValueFields.stream().map(DataField::id).collect(Collectors.toList());
            List<Integer> dataValueFieldIdList =
                    dataValueFields.stream().map(DataField::id).collect(Collectors.toList());
            for (int i = tableKeyCount + 2; i < tableProjection.length; i++) {
                int fieldId =
                        tableValueFieldIdList.get(tableProjection[i] - tableKeyFields.size() - 2);
                int dataFieldIndex = dataValueFieldIdList.indexOf(fieldId);
                int dataValueIndex = Ints.indexOf(dataValueProjection, dataFieldIndex);
                indexMapping[i] =
                        dataValueIndex < 0 ? dataValueIndex : dataValueIndex + dataKeyCount + 2;
            }
        }

        for (int i = 0; i < indexMapping.length; i++) {
            if (indexMapping[i] != i) {
                return indexMapping;
            }
        }
        return null;
    }

    /**
     * Create data projection from table projection.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param tableProjection the projection of table
     * @return the projection of data
     */
    public static int[][] createDataProjection(
            List<DataField> tableFields, List<DataField> dataFields, int[][] tableProjection) {
        List<Integer> tableFieldIdList =
                tableFields.stream().map(DataField::id).collect(Collectors.toList());
        List<Integer> dataFieldIdList =
                dataFields.stream().map(DataField::id).collect(Collectors.toList());
        return Arrays.stream(tableProjection)
                .map(p -> Arrays.copyOf(p, p.length))
                .peek(
                        p -> {
                            int fieldId = tableFieldIdList.get(p[0]);
                            p[0] = dataFieldIdList.indexOf(fieldId);
                        })
                .filter(p -> p[0] >= 0)
                .toArray(int[][]::new);
    }

    /**
     * Create predicate list from data fields.
     *
     * @param tableFields the table fields
     * @param dataFields the underlying data fields
     * @param filters the filters
     * @return the data filters
     */
    public static List<Predicate> createDataFilters(
            List<DataField> tableFields, List<DataField> dataFields, List<Predicate> filters) {
        if (filters == null) {
            return null;
        }

        List<Predicate> dataFilters = new ArrayList<>(filters.size());
        for (Predicate predicate : filters) {
            dataFilters.add(createDataPredicate(tableFields, dataFields, predicate));
        }
        return dataFilters;
    }

    @Nullable
    private static Predicate createDataPredicate(
            List<DataField> tableFields, List<DataField> dataFields, Predicate predicate) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = compoundPredicate.children();
            List<Predicate> dataChildren = new ArrayList<>(children.size());
            for (Predicate child : children) {
                Predicate dataPredicate = createDataPredicate(tableFields, dataFields, child);
                if (dataPredicate != null) {
                    dataChildren.add(dataPredicate);
                }
            }
            return new CompoundPredicate(compoundPredicate.function(), dataChildren);
        } else if (predicate instanceof LeafPredicate) {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            List<DataField> predicateTableFields =
                    tableFields.stream()
                            .filter(f -> f.name().equals(leafPredicate.fieldName()))
                            .collect(Collectors.toList());
            if (predicateTableFields.size() != 1) {
                throw new IllegalArgumentException(
                        String.format("Find none or multiple fields %s", predicateTableFields));
            }
            DataField tableField = predicateTableFields.get(0);
            List<DataField> predicateDataFields =
                    dataFields.stream()
                            .filter(f -> f.id() == tableField.id())
                            .collect(Collectors.toList());
            if (predicateDataFields.isEmpty()) {
                return null;
            } else if (predicateDataFields.size() > 1) {
                throw new IllegalArgumentException(
                        String.format("Find none or multiple fields %s", predicateTableFields));
            }
            DataField dataField = predicateDataFields.get(0);
            return new LeafPredicate(
                    leafPredicate.function(),
                    leafPredicate.type(),
                    dataFields.indexOf(dataField),
                    dataField.name(),
                    leafPredicate.literals());
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Not support to create data predicate from %s", predicate.getClass()));
        }
    }
}
