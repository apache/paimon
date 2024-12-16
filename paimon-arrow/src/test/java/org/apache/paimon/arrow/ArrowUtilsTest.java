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

package org.apache.paimon.arrow;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

/** Test for {@link ArrowUtils}. */
public class ArrowUtilsTest {

    private static final Random RANDOM = new Random();

    @Test
    public void testParquetFieldId() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.column("f4", DataTypes.DOUBLE());
        schemaBuilder.column("f5", DataTypes.STRING());
        schemaBuilder.column("F6", DataTypes.STRING());
        schemaBuilder.column("f7", DataTypes.BOOLEAN());
        schemaBuilder.column("f8", DataTypes.DATE());
        schemaBuilder.column("f10", DataTypes.TIMESTAMP(6));
        schemaBuilder.column("f11", DataTypes.DECIMAL(7, 2));
        schemaBuilder.column("f12", DataTypes.BYTES());
        schemaBuilder.column("f13", DataTypes.FLOAT());
        schemaBuilder.column("f14", DataTypes.BINARY(10));
        schemaBuilder.column("f15", DataTypes.VARBINARY(10));
        schemaBuilder.column(
                "f16",
                DataTypes.ARRAY(
                        DataTypes.ROW(
                                DataTypes.FIELD(0, "f0", DataTypes.INT()),
                                DataTypes.FIELD(1, "f1", DataTypes.SMALLINT()),
                                DataTypes.FIELD(2, "f2", DataTypes.STRING()),
                                DataTypes.FIELD(3, "f3", DataTypes.DOUBLE()),
                                DataTypes.FIELD(4, "f4", DataTypes.BOOLEAN()),
                                DataTypes.FIELD(5, "f5", DataTypes.DATE()),
                                DataTypes.FIELD(6, "f6", DataTypes.TIMESTAMP(6)),
                                DataTypes.FIELD(7, "f7", DataTypes.DECIMAL(7, 2)),
                                DataTypes.FIELD(8, "f8", DataTypes.BYTES()),
                                DataTypes.FIELD(9, "f9", DataTypes.FLOAT()),
                                DataTypes.FIELD(10, "f10", DataTypes.BINARY(10)))));

        RowType rowType = schemaBuilder.build().rowType();

        List<Field> fields =
                ArrowUtils.createVectorSchemaRoot(rowType, new RootAllocator())
                        .getSchema()
                        .getFields();

        for (int i = 0; i < 16; i++) {
            Assertions.assertThat(
                            Integer.parseInt(
                                    fields.get(i).getMetadata().get(ArrowUtils.PARQUET_FIELD_ID)))
                    .isEqualTo(i);
        }

        fields = fields.get(15).getChildren().get(0).getChildren();
        for (int i = 16; i < 26; i++) {
            Assertions.assertThat(
                            Integer.parseInt(
                                    fields.get(i - 16)
                                            .getMetadata()
                                            .get(ArrowUtils.PARQUET_FIELD_ID)))
                    .isEqualTo(i);
        }
    }
}
