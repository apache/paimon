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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.predicate.AlwaysFalse;
import org.apache.flink.table.store.file.predicate.AlwaysTrue;
import org.apache.flink.table.store.file.predicate.CompoundPredicate;
import org.apache.flink.table.store.file.predicate.IsNull;
import org.apache.flink.table.store.file.predicate.LeafPredicate;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.utils.ProjectedRowData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

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
     * data fields, 1 is the index of 6->b in data fields and 1 is the index of 3->a in data fields.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
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
     * Create index mapping from table projection to underlying data projection. For example, the
     * table and data fields are as follows
     *
     * <ul>
     *   <li>table fields: 1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>data fields: 1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>The table and data top projections are as follows
     *
     * <ul>
     *   <li>table projection: [0, 4, 1]
     *   <li>data projection: [0, 2]
     * </ul>
     *
     * <p>We can first get fields list for table and data projections from their fields as follows
     *
     * <ul>
     *   <li>table projection field list: [1->c, 6->b, 3->a]
     *   <li>data projection field list: [1->a, 3->c]
     * </ul>
     *
     * <p>Then create index mapping based on the fields list.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
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
        List<DataField> tableProjectFields = new ArrayList<>(tableProjection.length);
        for (int index : tableProjection) {
            tableProjectFields.add(tableFields.get(index));
        }

        List<DataField> dataProjectFields = new ArrayList<>(dataProjection.length);
        for (int index : dataProjection) {
            dataProjectFields.add(dataFields.get(index));
        }

        return createIndexMapping(tableProjectFields, dataProjectFields);
    }

    /**
     * Create index mapping from table projection to data with key and value fields. We should first
     * create table and data fields with their key/value fields, then create index mapping with
     * their projections and fields. For example, the table and data projections and fields are as
     * follows
     *
     * <ul>
     *   <li>Table key fields: 1->ka, 3->kb, 5->kc, 6->kd; value fields: 0->a, 2->d, 4->b;
     *       projection: [0, 2, 3, 4, 5, 7] where 0 is 1->ka, 2 is 5->kc, 3 is 5->kc, 4/5 are seq
     *       and kind, 7 is 2->d
     *   <li>Data key fields: 1->kb, 5->ka; value fields: 2->aa, 4->f; projection: [0, 1, 2, 3, 4]
     *       where 0 is 1->kb, 1 is 5->ka, 2/3 are seq and kind, 4 is 2->aa
     * </ul>
     *
     * <p>First we will get max key id from table and data fields which is 6, then create table and
     * data fields on it
     *
     * <ul>
     *   <li>Table fields: 1->ka, 3->kb, 5->kc, 6->kd, 7->seq, 8->kind, 9->a, 11->d, 13->b
     *   <li>Data fields: 1->kb, 5->ka, 7->seq, 8->kind, 11->aa, 13->f
     * </ul>
     *
     * <p>Finally we can create index mapping with table/data projections and fields.
     *
     * <p>/// TODO should support nest index mapping when nest schema evolution is supported.
     *
     * @param tableProjection the table projection
     * @param tableKeyFields the table key fields
     * @param tableValueFields the table value fields
     * @param dataProjection the data projection
     * @param dataKeyFields the data key fields
     * @param dataValueFields the data value fields
     * @return the result index mapping
     */
    @Nullable
    public static int[] createIndexMapping(
            int[] tableProjection,
            List<DataField> tableKeyFields,
            List<DataField> tableValueFields,
            int[] dataProjection,
            List<DataField> dataKeyFields,
            List<DataField> dataValueFields) {
        int maxKeyId =
                Math.max(
                        tableKeyFields.stream().mapToInt(DataField::id).max().orElse(0),
                        dataKeyFields.stream().mapToInt(DataField::id).max().orElse(0));
        List<DataField> tableFields =
                KeyValue.createKeyValueFields(tableKeyFields, tableValueFields, maxKeyId);
        List<DataField> dataFields =
                KeyValue.createKeyValueFields(dataKeyFields, dataValueFields, maxKeyId);
        return createIndexMapping(tableProjection, tableFields, dataProjection, dataFields);
    }

    /**
     * Create data projection from table projection. For example, the table and data fields are as
     * follows
     *
     * <ul>
     *   <li>table fields: 1->c, 3->a, 4->e, 5->d, 6->b
     *   <li>data fields: 1->a, 2->b, 3->c, 4->d
     * </ul>
     *
     * <p>When we project 1->c, 6->b, 3->a from table fields, the table projection is [[0], [4],
     * [1]], in which 0 is the index of field 1->c, 4 is the index of field 6->b, 1 is the index of
     * field 3->a in table fields. We need to create data projection from [[0], [4], [1]] as
     * follows:
     *
     * <ul>
     *   <li>Get field id of each index in table projection from table fields
     *   <li>Get index of each field above from data fields
     * </ul>
     *
     * <p>The we can create table projection as follows: [[0], [-1], [2]], in which 0, -1 and 2 are
     * the index of fields [1->c, 6->b, 3->a] in data fields. When we project column from underlying
     * data, we need to specify the field index and name. It is difficult to assign a proper field
     * id and name for 6->b in data projection and add it to data fields, and we can't use 6->b
     * directly because the field index of b in underlying is 2. We can remove the -1 field index in
     * data projection, then the result data projection is: [[0], [2]].
     *
     * <p>We create {@link RowData} for 1->a, 3->c after projecting them from underlying data, then
     * create {@link ProjectedRowData} with a index mapping and return null for 6->b in table
     * fields.
     *
     * @param tableFields the fields of table
     * @param dataFields the fields of underlying data
     * @param tableProjection the projection of table
     * @return the projection of data
     */
    public static int[][] createDataProjection(
            List<DataField> tableFields, List<DataField> dataFields, int[][] tableProjection) {
        List<Integer> dataFieldIdList =
                dataFields.stream().map(DataField::id).collect(Collectors.toList());
        return Arrays.stream(tableProjection)
                .map(p -> Arrays.copyOf(p, p.length))
                .peek(
                        p -> {
                            int fieldId = tableFields.get(p[0]).id();
                            p[0] = dataFieldIdList.indexOf(fieldId);
                        })
                .filter(p -> p[0] >= 0)
                .toArray(int[][]::new);
    }

    /**
     * Create predicate list from data fields. We will visit all predicate in filters, reset it's
     * field index, name and type, and use {@link AlwaysFalse} or {@link AlwaysTrue} if the field is
     * not exist.
     *
     * @param tableFields the table fields
     * @param dataFields the underlying data fields
     * @param filters the filters
     * @return the data filters
     */
    @Nullable
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

    private static Predicate createDataPredicate(
            List<DataField> tableFields, List<DataField> dataFields, Predicate predicate) {
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = compoundPredicate.children();
            List<Predicate> dataChildren = new ArrayList<>(children.size());
            for (Predicate child : children) {
                Predicate dataPredicate = createDataPredicate(tableFields, dataFields, child);
                dataChildren.add(dataPredicate);
            }
            return new CompoundPredicate(compoundPredicate.function(), dataChildren);
        } else if (predicate instanceof LeafPredicate) {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            Map<Integer, DataField> idToDataFields =
                    dataFields.stream().collect(Collectors.toMap(DataField::id, f -> f));
            Map<String, DataField> nameToTableFields =
                    tableFields.stream().collect(Collectors.toMap(DataField::name, f -> f));

            DataField tableField =
                    checkNotNull(
                            nameToTableFields.get(leafPredicate.fieldName()),
                            String.format("Find no field %s", leafPredicate.fieldName()));
            DataField dataField = idToDataFields.get(tableField.id());
            if (dataField == null) {
                // The table field is not exist in data fields, check the predicate function
                if (leafPredicate.function() instanceof IsNull) {
                    // Just get the first value
                    return new LeafPredicate(
                            AlwaysTrue.INSTANCE,
                            leafPredicate.type(),
                            0,
                            null,
                            leafPredicate.literals());
                } else {
                    return new LeafPredicate(
                            AlwaysFalse.INSTANCE,
                            leafPredicate.type(),
                            0,
                            null,
                            leafPredicate.literals());
                }
            }

            /// TODO Should deal with column type schema evolution here
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
