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

package org.apache.paimon.iceberg.metadata;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IcebergSchema}. */
public class IcebergSchemaTest {

    @Test
    public void testFieldIdsArePositiveAndUniqueForNestedTypes() {
        // Paimon assigns 0-based ids to top-level and nested fields from a single counter:
        // a=0, s=1, x=2, y=3.
        RowType nested =
                new RowType(
                        Arrays.asList(
                                new DataField(2, "x", DataTypes.INT()),
                                new DataField(3, "y", DataTypes.INT())));
        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "a", DataTypes.INT()),
                        new DataField(1, "s", nested),
                        new DataField(4, "arr", new ArrayType(DataTypes.INT())));

        IcebergSchema schema =
                IcebergSchema.create(
                        new TableSchema(
                                0L,
                                fields,
                                5,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));

        List<Integer> ids = new ArrayList<>();
        collectIds(schema.fields(), ids);

        // every emitted id is positive (Iceberg requires >= 1) ...
        assertThat(ids).allMatch(id -> id >= 1);
        // ... and unique, so an id-based lookup has a single answer.
        Set<Integer> unique = new HashSet<>(ids);
        assertThat(unique).hasSameSizeAs(ids);
        // the top-level ids keep the Paimon order, shifted by one.
        assertThat(schema.fields().get(0).id()).isEqualTo(1);
        assertThat(schema.fields().get(1).id()).isEqualTo(2);
        // the nested struct ids follow their parent and are shifted too, so no id is reused.
        IcebergStructType struct = (IcebergStructType) schema.fields().get(1).type();
        assertThat(struct.fields().stream().map(IcebergDataField::id).collect(Collectors.toList()))
                .containsExactly(3, 4);
        // the array element id stays positive and distinct from the row fields.
        IcebergListType list = (IcebergListType) schema.fields().get(2).type();
        assertThat(list.elementId()).isGreaterThan(4);
    }

    private static void collectIds(List<IcebergDataField> fields, List<Integer> ids) {
        for (IcebergDataField field : fields) {
            ids.add(field.id());
            Object type = field.type();
            if (type instanceof IcebergStructType) {
                collectIds(((IcebergStructType) type).fields(), ids);
            }
        }
    }
}
