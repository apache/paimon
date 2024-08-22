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

package org.apache.paimon.arrow.vector;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** Test for {@link StaticElementConverter}. */
public class StaticElementConverterTest {

    @Test
    public void testFunction() {
        try (RootAllocator rootAllocator = new RootAllocator()) {
            DataField dataField = new DataField(0, "id", DataTypes.STRING());
            GenericRow genericRow = new GenericRow(1);
            genericRow.setField(0, BinaryString.fromString("aklsdfjaklfjasklfd"));
            StaticElementConverter staticElementConverter =
                    new StaticElementConverter(rootAllocator, dataField, genericRow);
            try (FieldVector fieldVector = staticElementConverter.generate(10000)) {
                ArrowBatchReader reader =
                        new ArrowBatchReader(new RowType(Arrays.asList(dataField)));
                Iterable<InternalRow> it =
                        reader.readBatch(new VectorSchemaRoot(Arrays.asList(fieldVector)));
                it.forEach(
                        i ->
                                Assertions.assertThat(i.getString(0))
                                        .isEqualTo(genericRow.getString(0)));
            }
        }

        try (RootAllocator rootAllocator = new RootAllocator()) {
            DataField dataField = new DataField(0, "id", DataTypes.INT());
            GenericRow genericRow = new GenericRow(1);
            genericRow.setField(0, 10086);
            StaticElementConverter staticElementConverter =
                    new StaticElementConverter(rootAllocator, dataField, genericRow);
            try (FieldVector fieldVector = staticElementConverter.generate(10000)) {
                ArrowBatchReader reader =
                        new ArrowBatchReader(new RowType(Arrays.asList(dataField)));
                Iterable<InternalRow> it =
                        reader.readBatch(new VectorSchemaRoot(Arrays.asList(fieldVector)));
                it.forEach(i -> Assertions.assertThat(i.getInt(0)).isEqualTo(genericRow.getInt(0)));
            }
        }

        try (RootAllocator rootAllocator = new RootAllocator()) {
            DataField dataField = new DataField(0, "id", DataTypes.TIMESTAMP(6));
            GenericRow genericRow = new GenericRow(1);
            genericRow.setField(0, Timestamp.fromEpochMillis(10086));
            StaticElementConverter staticElementConverter =
                    new StaticElementConverter(rootAllocator, dataField, genericRow);
            try (FieldVector fieldVector = staticElementConverter.generate(100000)) {
                Assertions.assertThat(fieldVector.getValueCount()).isEqualTo(100000);
                ArrowBatchReader reader =
                        new ArrowBatchReader(new RowType(Arrays.asList(dataField)));
                Iterable<InternalRow> it =
                        reader.readBatch(new VectorSchemaRoot(Arrays.asList(fieldVector)));
                it.forEach(
                        i ->
                                Assertions.assertThat(i.getTimestamp(0, 6))
                                        .isEqualTo(genericRow.getTimestamp(0, 6)));
            }
        }
    }
}
