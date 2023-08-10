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

package org.apache.paimon.format.parquet;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.paimon.format.parquet.ParquetSchemaConverter.LIST_ELEMENT_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.LIST_REPEATED_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_KEY_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_REPEATED_NAME;
import static org.apache.paimon.format.parquet.ParquetSchemaConverter.MAP_VALUE_NAME;

/** convert Paimon Row to Parquet FilePath. */
public class RowtypeToFieldPathConverter {
    public static List<String> getAllFieldPath(RowType type) {
        List<String> result = new ArrayList<>();

        LinkedList<Pair<String, DataType>> queue = new LinkedList<>();
        List<DataField> fields = type.getFields();
        for (DataField field : fields) {
            queue.offer(Pair.of(field.name(), field.type()));
        }
        while (!queue.isEmpty()) {
            Pair<String, DataType> polled = queue.poll();
            String fieldName = polled.getLeft();
            DataType dataType = polled.getRight();

            if (dataType.getTypeRoot() == DataTypeRoot.ROW) {
                List<Pair<String, DataType>> pairs =
                        traversalRowType(fieldName, (RowType) dataType);
                for (Pair<String, DataType> pair : pairs) {
                    queue.offer(pair);
                }
            } else if (dataType.getTypeRoot() == DataTypeRoot.MAP) {
                result.add(String.format("%s.%s.%s", fieldName, MAP_REPEATED_NAME, MAP_KEY_NAME));
                result.add(String.format("%s.%s.%s", fieldName, MAP_REPEATED_NAME, MAP_VALUE_NAME));
            } else if (dataType.getTypeRoot() == DataTypeRoot.MULTISET) {
                result.add(String.format("%s.%s.%s", fieldName, MAP_REPEATED_NAME, MAP_KEY_NAME));
            } else if (dataType.getTypeRoot() == DataTypeRoot.ARRAY) {
                result.add(
                        String.format(
                                "%s.%s.%s", fieldName, LIST_REPEATED_NAME, LIST_ELEMENT_NAME));
            } else {
                result.add(fieldName);
            }
        }
        return result;
    }

    public static List<Pair<String, DataType>> traversalRowType(String prefix, RowType type) {
        List<Pair<String, DataType>> result = new ArrayList<>();

        LinkedList<Pair<String, RowType>> queue = new LinkedList<>();
        queue.offer(Pair.of(prefix, type));
        while (!queue.isEmpty()) {
            Pair<String, RowType> polled = queue.poll();

            String pathPrefix = polled.getKey();
            List<DataField> datafields = polled.getValue().getFields();
            for (DataField dataField : datafields) {
                DataType dataType = dataField.type();
                String name = dataField.name();
                String path = String.format("%s.%s", pathPrefix, name);
                if (dataType.getTypeRoot() == DataTypeRoot.ROW) {
                    queue.offer(Pair.of(path, (RowType) dataType));
                } else {
                    result.add(Pair.of(path, dataType));
                }
            }
        }
        return result;
    }
}
