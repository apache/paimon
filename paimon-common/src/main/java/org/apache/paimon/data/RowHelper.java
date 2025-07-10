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

package org.apache.paimon.data;

import org.apache.paimon.data.BinaryWriter.ValueSetter;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.io.Serializable;
import java.util.List;

/** A keeper to keep {@link BinaryRow} from {@link InternalRow}. */
public class RowHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private final FieldGetter[] fieldGetters;
    private final ValueSetter[] valueSetters;
    private final boolean[] writeNulls;
    private final Serializer<?>[] serializers;

    private transient BinaryRow reuseRow;
    private transient BinaryRowWriter reuseWriter;

    public RowHelper(List<DataType> types) {
        this.fieldGetters = new FieldGetter[types.size()];
        this.valueSetters = new ValueSetter[types.size()];
        this.writeNulls = new boolean[types.size()];
        this.serializers =
                types.stream().map(InternalSerializers::create).toArray(Serializer[]::new);
        for (int i = 0; i < types.size(); i++) {
            DataType type = types.get(i);
            fieldGetters[i] = InternalRow.createFieldGetter(type, i);
            valueSetters[i] = BinaryWriter.createValueSetter(type, serializers[i]);
            if (type instanceof DecimalType) {
                writeNulls[i] = !Decimal.isCompact(DataTypeChecks.getPrecision(type));
            } else if (type instanceof TimestampType || type instanceof LocalZonedTimestampType) {
                writeNulls[i] = !Timestamp.isCompact(DataTypeChecks.getPrecision(type));
            }
        }
    }

    public void copyInto(InternalRow row) {
        if (reuseRow == null) {
            reuseRow = new BinaryRow(fieldGetters.length);
            reuseWriter = new BinaryRowWriter(reuseRow);
        }

        reuseWriter.reset();
        reuseWriter.writeRowKind(row.getRowKind());
        for (int i = 0; i < fieldGetters.length; i++) {
            Object field = fieldGetters[i].getFieldOrNull(row);
            if (field == null && !writeNulls[i]) {
                reuseWriter.setNullAt(i);
            } else {
                valueSetters[i].setValue(reuseWriter, i, field);
            }
        }
        reuseWriter.complete();
    }

    public BinaryRow reuseRow() {
        return reuseRow;
    }

    public BinaryRow copiedRow() {
        return reuseRow.copy();
    }

    @SuppressWarnings("rawtypes")
    public Serializer serializer(int i) {
        return serializers[i];
    }

    public FieldGetter fieldGetter(int i) {
        return fieldGetters[i];
    }
}
