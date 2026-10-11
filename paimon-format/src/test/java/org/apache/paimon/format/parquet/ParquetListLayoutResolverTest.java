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

package org.apache.paimon.format.parquet;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ParquetListLayoutResolver}.
 *
 * <p>All list layout decisions should be made through {@link ParquetListLayoutResolver} so that
 * schema inference, requested-schema clipping and reader construction share a single
 * interpretation; the layouts below follow the backward-compatibility rules in the Parquet spec.
 */
public class ParquetListLayoutResolverTest {

    @Test
    public void testResolveThreeLevelElementType() {
        // Rule 5: canonical three-level list (list -> element) with a primitive element.
        GroupType threeLevelPrimitiveList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                        .optional(INT32)
                                        .named("element")
                                        .named("list"))
                        .named("my_list");
        Type threeLevelPrimitiveElement =
                ParquetListLayoutResolver.resolveElementType(threeLevelPrimitiveList);
        assertThat(threeLevelPrimitiveElement.isPrimitive()).isEqualTo(true);
        assertThat(threeLevelPrimitiveElement.getName()).isEqualTo("element");

        // Rule 5: canonical three-level list (list -> element) with a group element.
        GroupType threeLevelGroupElement =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .optional(INT32)
                        .named("x")
                        .optional(INT32)
                        .named("y")
                        .named("element");
        GroupType threeLevelGroupList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(
                                Types.buildGroup(Type.Repetition.REPEATED)
                                        .addField(threeLevelGroupElement)
                                        .named("list"))
                        .named("my_list");
        Type threeLevelGroupElementType =
                ParquetListLayoutResolver.resolveElementType(threeLevelGroupList);
        assertThat(threeLevelGroupElementType.isPrimitive()).isEqualTo(false);
        assertThat(threeLevelGroupElementType.getName()).isEqualTo("element");
        assertThat(threeLevelGroupElementType.asGroupType().getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testResolveTwoLevelElementType() {
        // Rule 1: a repeated primitive field is itself the element type.
        GroupType twoLevelPrimitiveList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .repeated(INT32)
                        .named("element")
                        .named("my_list");
        Type twoLevelPrimitiveElement =
                ParquetListLayoutResolver.resolveElementType(twoLevelPrimitiveList);
        assertThat(twoLevelPrimitiveElement.isPrimitive()).isEqualTo(true);
        assertThat(twoLevelPrimitiveElement.getName()).isEqualTo("element");

        // Rule 2: a repeated group with multiple fields is itself the element type.
        GroupType twoLevelGroupElement =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("x")
                        .optional(INT32)
                        .named("y")
                        .named("element");
        GroupType twoLevelGroupList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(twoLevelGroupElement)
                        .named("my_list");
        Type twoLevelGroupElementType =
                ParquetListLayoutResolver.resolveElementType(twoLevelGroupList);
        assertThat(twoLevelGroupElementType.isPrimitive()).isEqualTo(false);
        assertThat(twoLevelGroupElementType.getName()).isEqualTo("element");
        assertThat(twoLevelGroupElementType.asGroupType().getFieldCount()).isEqualTo(2);
    }

    @Test
    public void testResolveLegacyNestedElementType() {
        // Rule 3: a repeated group with a single repeated field is the element type.
        GroupType nestedRepeatedWrapper =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .repeated(INT32)
                        .named("array")
                        .named("array");
        GroupType nestedRepeatedList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(nestedRepeatedWrapper)
                        .named("my_list");
        Type nestedRepeatedElement =
                ParquetListLayoutResolver.resolveElementType(nestedRepeatedList);
        assertThat(nestedRepeatedElement.isPrimitive()).isEqualTo(false);
        assertThat(nestedRepeatedElement.getName()).isEqualTo("array");
        assertThat(nestedRepeatedElement.asGroupType().getFieldCount()).isEqualTo(1);
        assertThat(nestedRepeatedElement.asGroupType().getType(0).getName()).isEqualTo("array");
    }

    @Test
    public void testResolveLegacyNamedElementType() {
        // Rule 4: a repeated group named "array" with one field is the element type.
        GroupType arrayWrapper =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("foo")
                        .named("array");
        GroupType arrayWrapperList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(arrayWrapper)
                        .named("my_list");
        Type arrayWrapperElement = ParquetListLayoutResolver.resolveElementType(arrayWrapperList);
        assertThat(arrayWrapperElement.isPrimitive()).isEqualTo(false);
        assertThat(arrayWrapperElement.getName()).isEqualTo("array");

        // Rule 4: a repeated group named "<list>_tuple" with one field is the element type.
        GroupType tupleWrapper =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .required(BINARY)
                        .as(LogicalTypeAnnotation.stringType())
                        .named("str")
                        .named("my_list_tuple");
        GroupType tupleWrapperList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(tupleWrapper)
                        .named("my_list");
        Type tupleWrapperElement = ParquetListLayoutResolver.resolveElementType(tupleWrapperList);
        assertThat(tupleWrapperElement.isPrimitive()).isEqualTo(false);
        assertThat(tupleWrapperElement.getName()).isEqualTo("my_list_tuple");
        assertThat(tupleWrapperElement.asGroupType().getFieldCount()).isEqualTo(1);
        assertThat(tupleWrapperElement.asGroupType().getType(0).getName()).isEqualTo("str");
    }

    @Test
    public void testResolveLegacyBagElementType() {
        // Rule 5: a repeated group with a single non-repeated field that is neither "array" nor
        // "<list>_tuple" unwraps to the single child (e.g. Hive's bag/array_element encoding).
        GroupType bagWrapper =
                Types.buildGroup(Type.Repetition.REPEATED)
                        .optional(INT32)
                        .named("array_element")
                        .named("bag");
        GroupType bagWrapperList =
                Types.buildGroup(Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.listType())
                        .addField(bagWrapper)
                        .named("my_list");
        assertThat(ParquetListLayoutResolver.isThreeLevelList(bagWrapperList)).isTrue();
        assertThat(ParquetListLayoutResolver.isCanonicalList(bagWrapperList)).isFalse();
        Type bagWrapperElement = ParquetListLayoutResolver.resolveElementType(bagWrapperList);
        assertThat(bagWrapperElement.isPrimitive()).isEqualTo(true);
        assertThat(bagWrapperElement.getName()).isEqualTo("array_element");
    }
}
