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

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** A data type that contains field data types. */
public class RowDataType extends DataType {

    private static final long serialVersionUID = 1L;

    private final List<DataField> fields;

    public RowDataType(List<DataField> fields) {
        this(true, fields);
    }

    public RowDataType(boolean isNullable, List<DataField> fields) {
        super(toRowType(isNullable, fields));
        this.fields = fields;
    }

    public static RowType toRowType(boolean isNullable, List<DataField> fields) {
        List<RowType.RowField> typeFields = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            typeFields.add(
                    new RowType.RowField(
                            field.name(), field.type().logicalType, field.description()));
        }
        return new RowType(isNullable, typeFields);
    }

    public List<DataField> fields() {
        return fields;
    }

    @Override
    public DataType copy(boolean isNullable) {
        return new RowDataType(isNullable, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        RowDataType that = (RowDataType) o;
        return Objects.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields);
    }
}
