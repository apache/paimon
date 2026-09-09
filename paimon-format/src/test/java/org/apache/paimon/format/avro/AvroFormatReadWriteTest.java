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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatReadWriteTest;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** An avro {@link FormatReadWriteTest}. */
public class AvroFormatReadWriteTest extends FormatReadWriteTest {

    protected AvroFormatReadWriteTest() {
        super("avro");
    }

    @Override
    protected FileFormat fileFormat() {
        return new AvroFileFormat(new FileFormatFactory.FormatContext(new Options(), 1024, 1024));
    }

    @Test
    public void testArrayBlobDescriptors() throws Exception {
        testArrayBlobDescriptorRoundTrip();
    }

    @Test
    public void testMultisetWithNonStringElement() throws IOException {
        // a non-string element makes the multiset use the array-of-record encoding
        testMultisetRoundTrip(DataTypes.INT().notNull(), 10, 20);
    }

    @Test
    public void testMultisetWithStringElement() throws IOException {
        // a string element makes the multiset use the native avro map encoding
        testMultisetRoundTrip(
                DataTypes.STRING().notNull(),
                BinaryString.fromString("a"),
                BinaryString.fromString("b"));
    }

    private void testMultisetRoundTrip(DataType elementType, Object first, Object second)
            throws IOException {
        RowType rowType =
                RowType.builder()
                        .field("id", DataTypes.INT().notNull())
                        .field("ms", DataTypes.MULTISET(elementType))
                        .build();
        Map<Object, Object> multiset = new HashMap<>();
        multiset.put(first, 1);
        multiset.put(second, 2);
        GenericRow expected = GenericRow.of(1, new GenericMap(multiset));

        FileFormat format = fileFormat();
        write(format.createWriterFactory(rowType), file, expected);

        List<InternalRow> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                format.createReaderFactory(rowType, rowType, new ArrayList<>())
                        .createReader(
                                new FormatReaderContext(
                                        fileIO, file, fileIO.getFileSize(file), null, null))) {
            InternalRowSerializer serializer = new InternalRowSerializer(rowType);
            reader.forEachRemaining(row -> result.add(serializer.copy(row)));
        }

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getInt(0)).isEqualTo(1);
        InternalMap actual = result.get(0).getMap(1);
        assertThat(actual.size()).isEqualTo(multiset.size());
        InternalArray.ElementGetter keyGetter = InternalArray.createElementGetter(elementType);
        InternalArray keys = actual.keyArray();
        InternalArray values = actual.valueArray();
        Map<Object, Object> actualMultiset = new HashMap<>();
        for (int i = 0; i < actual.size(); i++) {
            actualMultiset.put(keyGetter.getElementOrNull(keys, i), values.getInt(i));
        }
        assertThat(actualMultiset).isEqualTo(multiset);
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
