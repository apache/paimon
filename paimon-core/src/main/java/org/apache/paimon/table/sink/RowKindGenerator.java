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

package org.apache.paimon.table.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Generate row kind. */
public class RowKindGenerator {

    private final int index;

    public RowKindGenerator(String field, RowType rowType) {
        this.index = rowType.getFieldNames().indexOf(field);
        if (index == -1) {
            throw new RuntimeException(
                    String.format("Can not find rowkind %s in table schema: %s", field, rowType));
        }
        DataType fieldType = rowType.getTypeAt(index);
        checkArgument(
                fieldType.is(CHARACTER_STRING),
                "only support string type for rowkind, but %s is %s",
                field,
                fieldType);
    }

    public RowKind generate(InternalRow row) {
        if (row.isNullAt(index)) {
            throw new RuntimeException("Row kind cannot be null.");
        }
        return RowKind.fromShortString(row.getString(index).toString());
    }

    @Nullable
    public static RowKindGenerator create(TableSchema schema, CoreOptions options) {
        return options.rowkindField()
                .map(field -> new RowKindGenerator(field, schema.logicalRowType()))
                .orElse(null);
    }
}
