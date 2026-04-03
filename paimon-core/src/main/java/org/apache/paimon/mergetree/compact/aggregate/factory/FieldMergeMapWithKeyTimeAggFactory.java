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

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldMergeMapWithKeyTimeAgg;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Factory for {@link FieldMergeMapWithKeyTimeAgg}. */
public class FieldMergeMapWithKeyTimeAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "merge_map_with_keytime";

    @Override
    public FieldMergeMapWithKeyTimeAgg create(
            DataType fieldType, CoreOptions options, String field) {
        checkArgument(
                fieldType instanceof MapType,
                "Data type for field '%s' must be 'MAP' but was '%s'",
                field,
                fieldType);

        MapType mapType = (MapType) fieldType;
        DataType valueType = mapType.getValueType();

        checkArgument(
                valueType instanceof RowType,
                "Value type of MAP for field '%s' must be ROW but was '%s'",
                field,
                valueType);

        RowType rowType = (RowType) valueType;
        checkArgument(
                rowType.getFieldCount() >= 2,
                "ROW type for field '%s' must have at least 2 fields, but found %s",
                field,
                rowType.getFieldCount());

        int tsFieldIndex = resolveTsFieldIndex(rowType, options, field);
        return new FieldMergeMapWithKeyTimeAgg(NAME, mapType, tsFieldIndex);
    }

    private int resolveTsFieldIndex(RowType rowType, CoreOptions options, @Nullable String field) {
        String tsFieldName = options.fieldMergeMapTsField(field);
        int tsFieldIndex;
        if (tsFieldName == null) {
            // default to the last field
            tsFieldIndex = rowType.getFieldCount() - 1;
        } else {
            tsFieldIndex = rowType.getFieldIndex(tsFieldName);
            checkArgument(
                    tsFieldIndex >= 0,
                    "Timestamp field '%s' not found in ROW type for field '%s'. Available fields: %s",
                    tsFieldName,
                    field,
                    rowType.getFieldNames());
        }
        return tsFieldIndex;
    }

    @Override
    public String identifier() {
        return NAME;
    }
}
