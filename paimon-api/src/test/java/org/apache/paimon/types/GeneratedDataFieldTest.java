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

package org.apache.paimon.types;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that physical generated columns retain their definition across schema operations. */
class GeneratedDataFieldTest {

    @Test
    void testSchemaRoundTripAndCopies() {
        Schema schema =
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .generatedColumn("dt", DataTypes.DATE(), "CAST(ts AS DATE)")
                        .partitionKeys("dt")
                        .primaryKey("ts", "dt")
                        .build();
        DataField generated = schema.fields().get(1);
        assertThat(generated.generationExpression()).isEqualTo("CAST(ts AS DATE)");
        TableSchema tableSchema =
                new TableSchema(
                        0,
                        schema.fields(),
                        1,
                        schema.partitionKeys(),
                        schema.primaryKeys(),
                        Collections.emptyMap(),
                        null);
        assertThat(TableSchema.fromJson(tableSchema.toString())).isEqualTo(tableSchema);
        assertThat(generated.copy()).isEqualTo(generated);
        assertThat(generated.copy(true).generationExpression()).isEqualTo("CAST(ts AS DATE)");
        assertThat(generated.newId(99).generationExpression()).isEqualTo("CAST(ts AS DATE)");
        assertThat(generated.newName("day").generationExpression()).isEqualTo("CAST(ts AS DATE)");
        assertThat(generated.newType(DataTypes.DATE()).generationExpression())
                .isEqualTo("CAST(ts AS DATE)");
        assertThat(generated.newDescription("partition").generationExpression())
                .isEqualTo("CAST(ts AS DATE)");
        assertThat(generated.asSQLString()).contains("GENERATED ALWAYS AS (CAST(ts AS DATE))");
        RowType reassigned =
                (RowType) ReassignFieldId.reassign(schema.rowType(), new AtomicInteger(20));
        assertThat(reassigned.getField("dt").generationExpression()).isEqualTo("CAST(ts AS DATE)");
        assertThat(generated).isNotEqualTo(generated.newGenerationExpression(null));
    }

    @Test
    void testLegacySchemaHasNoGenerationExpression() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Collections.singletonList(new DataField(0, "id", DataTypes.INT())),
                        0,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        null);
        assertThat(schema.toString()).doesNotContain("generationExpression");
        assertThat(TableSchema.fromJson(schema.toString()).fields().get(0).generationExpression())
                .isNull();
    }

    @Test
    void testExplicitNullGenerationExpression() {
        TableSchema schema =
                TableSchema.fromJson(
                        "{\"version\":3,\"id\":0,\"fields\":[{\"id\":0,\"name\":\"id\",\"type\":\"INT\","
                                + "\"generationExpression\":null}],\"highestFieldId\":0,\"partitionKeys\":[],"
                                + "\"primaryKeys\":[],\"options\":{},\"timeMillis\":0}");
        assertThat(schema.fields().get(0).generationExpression()).isNull();
    }
}
