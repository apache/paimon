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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** An avro {@link FormatReadWriteTest}. */
public class AvroFormatReadWriteTest extends FormatReadWriteTest {

    protected AvroFormatReadWriteTest() {
        super("avro");
    }

    @Override
    protected FileFormat fileFormat() {
        return new AvroFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Override
    protected RowType rowTypeForFullTypesTest() {
        RowType rowWithoutVector = super.rowTypeForFullTypesTest();
        List<DataField> fields = new ArrayList<>(rowWithoutVector.getFields());
        int vectorFieldId = fields.stream().map(DataField::id).max(Integer::compare).get() + 1;
        fields.add(new DataField(vectorFieldId, "embed", DataTypes.VECTOR(3, DataTypes.FLOAT())));
        return new RowType(rowWithoutVector.isNullable(), fields);
    }

    @Override
    protected GenericRow expectedRowForFullTypesTest() {
        float[] vector = new float[] {1.0f, 2.0f, 3.0f};
        GenericRow rowWithoutVector = super.expectedRowForFullTypesTest();
        GenericRow row = new GenericRow(rowWithoutVector.getFieldCount() + 1);
        for (int i = 0; i < rowWithoutVector.getFieldCount(); ++i) {
            row.setField(i, rowWithoutVector.getField(i));
        }
        row.setField(rowWithoutVector.getFieldCount(), BinaryVector.fromPrimitiveArray(vector));
        return row;
    }
}
