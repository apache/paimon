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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

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
        assertThat(variant.variantGet("$.object"))
                .isEqualTo(
                        "{\"address\":{\"city\":\"Hangzhou\",\"street\":\"Main St\"},\"age\":2,\"name\":\"Apache Paimon\"}");
        assertThat(variant.variantGet("$.object.name")).isEqualTo("Apache Paimon");
        assertThat(variant.variantGet("$.object.address.street")).isEqualTo("Main St");
        assertThat(variant.variantGet("$[\"object\"]['address'].city")).isEqualTo("Hangzhou");
        assertThat(variant.variantGet("$.array")).isEqualTo("[1,2,3,4,5]");
        assertThat(variant.variantGet("$.array[0]")).isEqualTo(1L);
        assertThat(variant.variantGet("$.array[3]")).isEqualTo(4L);
        assertThat(variant.variantGet("$.string")).isEqualTo("Hello, World!");
        assertThat(variant.variantGet("$.long")).isEqualTo(12345678901234L);
        assertThat(variant.variantGet("$.double"))
                .isEqualTo(1.0123456789012345678901234567890123456789);
        assertThat(variant.variantGet("$.decimal")).isEqualTo(new BigDecimal("100.99"));
        assertThat(variant.variantGet("$.boolean1")).isEqualTo(true);
        assertThat(variant.variantGet("$.boolean2")).isEqualTo(false);
        assertThat(variant.variantGet("$.nullField")).isNull();
    }
}
