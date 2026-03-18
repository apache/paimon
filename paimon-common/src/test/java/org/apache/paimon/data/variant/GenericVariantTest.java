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

package org.apache.paimon.data.variant;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.ZoneOffset;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.assembleVariant;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildVariantSchema;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.castShredded;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.apache.paimon.types.DataTypesTest.assertThat;

/** Test of {@link GenericVariant}. */
public class GenericVariantTest {

    @Test
    public void testToJson() {
        String json =
                "{\n"
                        + "  \"object\": {\n"
                        + "    \"name\": \"Apache Paimon\",\n"
                        + "    \"age\": 2,\n"
                        + "    \"address\": {\n"
                        + "      \"street\": \"Main St\",\n"
                        + "      \"city\": \"Hangzhou\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"array\": [1, 2, 3, 4, 5],\n"
                        + "  \"string\": \"Hello, World!\",\n"
                        + "  \"long\": 12345678901234,\n"
                        + "  \"double\": 1.0123456789012345678901234567890123456789,\n"
                        + "  \"decimal\": 100.99,\n"
                        + "  \"boolean1\": true,\n"
                        + "  \"boolean2\": false,\n"
                        + "  \"nullField\": null\n"
                        + "}\n";

        assertThat(GenericVariant.fromJson(json).toJson())
                .isEqualTo(
                        "{\"array\":[1,2,3,4,5],\"boolean1\":true,\"boolean2\":false,\"decimal\":100.99,\"double\":1.0123456789012346,\"long\":12345678901234,\"nullField\":null,\"object\":{\"address\":{\"city\":\"Hangzhou\",\"street\":\"Main St\"},\"age\":2,\"name\":\"Apache Paimon\"},\"string\":\"Hello, World!\"}");
    }

    @Test
    public void testVariantGet() {
        String json =
                "{\n"
                        + "  \"object\": {\n"
                        + "    \"name\": \"Apache Paimon\",\n"
                        + "    \"age\": 2,\n"
                        + "    \"address\": {\n"
                        + "      \"street\": \"Main St\",\n"
                        + "      \"city\": \"Hangzhou\"\n"
                        + "    }\n"
                        + "  },\n"
                        + "  \"array\": [1, 2, 3, 4, 5],\n"
                        + "  \"string\": \"Hello, World!\",\n"
                        + "  \"long\": 12345678901234,\n"
                        + "  \"double\": 1.0123456789012345678901234567890123456789,\n"
                        + "  \"decimal\": 100.99,\n"
                        + "  \"boolean1\": true,\n"
                        + "  \"boolean2\": false,\n"
                        + "  \"nullField\": null\n"
                        + "}\n";

        Variant variant = GenericVariant.fromJson(json);

        VariantCastArgs castArgs = new VariantCastArgs(false, ZoneOffset.UTC);
        assertThat(variant.variantGet("$.object", DataTypes.STRING(), castArgs))
                .isEqualTo(
                        BinaryString.fromString(
                                "{\"address\":{\"city\":\"Hangzhou\",\"street\":\"Main St\"},\"age\":2,\"name\":\"Apache Paimon\"}"));
        RowType address =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"street", "city"});
        assertThat(variant.variantGet("$.object.address", address, castArgs))
                .isEqualTo(
                        GenericRow.of(
                                BinaryString.fromString("Main St"),
                                BinaryString.fromString("Hangzhou")));
        assertThat(variant.variantGet("$.object.name", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("Apache Paimon"));
        assertThat(variant.variantGet("$.object.address.street", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("Main St"));
        assertThat(
                        variant.variantGet(
                                "$[\"object\"]['address'].city", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("Hangzhou"));
        assertThat(variant.variantGet("$.array", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("[1,2,3,4,5]"));
        assertThat(variant.variantGet("$.array", DataTypes.ARRAY(DataTypes.INT()), castArgs))
                .isEqualTo(new GenericArray(new Integer[] {1, 2, 3, 4, 5}));
        assertThat(variant.variantGet("$.array[0]", DataTypes.BIGINT(), castArgs)).isEqualTo(1L);
        assertThat(variant.variantGet("$.array[3]", DataTypes.BIGINT(), castArgs)).isEqualTo(4L);
        assertThat(variant.variantGet("$.string", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("Hello, World!"));
        assertThat(variant.variantGet("$.long", DataTypes.BIGINT(), castArgs))
                .isEqualTo(12345678901234L);
        assertThat(variant.variantGet("$.long", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("12345678901234"));
        assertThat(variant.variantGet("$.double", DataTypes.DOUBLE(), castArgs))
                .isEqualTo(1.0123456789012345678901234567890123456789);
        assertThat(variant.variantGet("$.decimal", DataTypes.DECIMAL(5, 2), castArgs))
                .isEqualTo(Decimal.fromBigDecimal(new BigDecimal("100.99"), 5, 2));
        assertThat(variant.variantGet("$.decimal", DataTypes.STRING(), castArgs))
                .isEqualTo(BinaryString.fromString("100.99"));
        assertThat(variant.variantGet("$.boolean1", DataTypes.BOOLEAN(), castArgs)).isEqualTo(true);
        assertThat(variant.variantGet("$.boolean2", DataTypes.BOOLEAN(), castArgs))
                .isEqualTo(false);
        assertThat(variant.variantGet("$.nullField", DataTypes.BOOLEAN(), castArgs)).isNull();
    }

    @Test
    public void testShredding() {
        GenericVariant variant = GenericVariant.fromJson("{\"a\": 1, \"b\": \"hello\"}");

        // Happy path
        RowType shreddedType1 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"a", "b"});
        GenericRow expert1 =
                GenericRow.of(
                        variant.metadata(),
                        null,
                        GenericRow.of(
                                GenericRow.of(null, 1),
                                GenericRow.of(null, BinaryString.fromString("hello"))));
        testShreddingResult(variant, shreddedType1, expert1);

        // Missing field
        RowType shreddedType2 =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"a", "c", "b"});
        GenericRow expert2 =
                GenericRow.of(
                        variant.metadata(),
                        null,
                        GenericRow.of(
                                GenericRow.of(null, 1),
                                GenericRow.of(null, null),
                                GenericRow.of(null, BinaryString.fromString("hello"))));
        testShreddingResult(variant, shreddedType2, expert2);

        // "a" is not present in shredding schema
        RowType shreddedType3 =
                RowType.of(
                        new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                        new String[] {"b", "c"});
        GenericRow expert3 =
                GenericRow.of(
                        variant.metadata(),
                        untypedValue("{\"a\": 1}"),
                        GenericRow.of(
                                GenericRow.of(null, BinaryString.fromString("hello")),
                                GenericRow.of(null, null)));
        testShreddingResult(variant, shreddedType3, expert3);
    }

    @Test
    public void testShreddingAllTypes() {
        String json =
                "{\n"
                        + "  \"c1\": \"Hello, World!\",\n"
                        + "  \"c2\": 12345678901234,\n"
                        + "  \"c3\": 1.0123456789012345678901234567890123456789,\n"
                        + "  \"c4\": 100.99,\n"
                        + "  \"c5\": true,\n"
                        + "  \"c6\": null,\n"
                        + "  \"c7\": {\"street\" : \"Main St\",\"city\" : \"Hangzhou\"},\n"
                        + "  \"c8\": [1, 2]\n"
                        + "}\n";
        GenericVariant variant = GenericVariant.fromJson(json);
        RowType shreddedType1 =
                RowType.of(
                        new DataType[] {
                            DataTypes.STRING(),
                            DataTypes.BIGINT(),
                            DataTypes.DOUBLE(),
                            DataTypes.DECIMAL(5, 2),
                            DataTypes.BOOLEAN(),
                            DataTypes.STRING(),
                            RowType.of(
                                    new DataType[] {DataTypes.STRING(), DataTypes.STRING()},
                                    new String[] {"street", "city"}),
                            DataTypes.ARRAY(DataTypes.INT())
                        },
                        new String[] {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"});
        GenericRow expert1 =
                GenericRow.of(
                        variant.metadata(),
                        null,
                        GenericRow.of(
                                GenericRow.of(null, BinaryString.fromString("Hello, World!")),
                                GenericRow.of(null, 12345678901234L),
                                GenericRow.of(null, 1.0123456789012345678901234567890123456789D),
                                GenericRow.of(
                                        null,
                                        Decimal.fromBigDecimal(new BigDecimal("100.99"), 5, 2)),
                                GenericRow.of(null, true),
                                GenericRow.of(new byte[] {0}, null),
                                GenericRow.of(
                                        null,
                                        GenericRow.of(
                                                GenericRow.of(
                                                        null, BinaryString.fromString("Main St")),
                                                GenericRow.of(
                                                        null,
                                                        BinaryString.fromString("Hangzhou")))),
                                GenericRow.of(
                                        null,
                                        new GenericArray(
                                                new GenericRow[] {
                                                    GenericRow.of(null, 1), GenericRow.of(null, 2)
                                                }))));
        testShreddingResult(variant, shreddedType1, expert1);

        // test no shredding
        RowType shreddedType2 =
                RowType.of(new DataType[] {DataTypes.STRING()}, new String[] {"other"});
        GenericRow expert2 =
                GenericRow.of(
                        variant.metadata(),
                        untypedValue(json),
                        GenericRow.of(GenericRow.of(null, null)));
        testShreddingResult(variant, shreddedType2, expert2);
    }

    private byte[] untypedValue(String input) {
        return GenericVariant.fromJson(input).value();
    }

    private void testShreddingResult(
            GenericVariant variant, RowType shreddedType, InternalRow expected) {
        RowType shreddingSchema = variantShreddingSchema(shreddedType);
        VariantSchema variantSchema = buildVariantSchema(shreddingSchema);
        // test cast shredded
        InternalRow shredded = castShredded(variant, variantSchema);
        assertThat(shredded).isEqualTo(expected);

        // test rebuild
        Variant rebuild = assembleVariant(shredded, variantSchema);
        assertThat(variant.toJson()).isEqualTo(rebuild.toJson());
    }
}
