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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;

import static org.apache.paimon.data.variant.PaimonShreddingUtils.buildVariantSchema;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.castShredded;
import static org.apache.paimon.data.variant.PaimonShreddingUtils.variantShreddingSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BaseVariantReader}. */
public class BaseVariantReaderTest {

    @Test
    void testRowReaderWithUnshreddedTargetField() {
        VariantCastArgs castArgs = new VariantCastArgs(true, ZoneOffset.UTC);

        // The file shreds only field "a" of the object, so a target asking for "b" as well
        // has to pick "b" up from the untyped value.
        RowType shreddedType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"a"});
        VariantSchema schema = buildVariantSchema(variantShreddingSchema(shreddedType));
        assertThat(schema.objectSchemaMap).containsKey("a").doesNotContainKey("b");

        RowType targetType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"a", "b"});
        BaseVariantReader reader = BaseVariantReader.create(schema, targetType, castArgs, false);

        InternalRow shredded =
                castShredded(GenericVariant.fromJson("{\"a\": 1, \"b\": \"hello\"}"), schema);
        assertThat(reader.read(shredded, shredded.getBinary(schema.topLevelMetadataIdx)))
                .isEqualTo(GenericRow.of(1, BinaryString.fromString("hello")));

        // "b" absent from both the shredded object and the untyped value reads back as null.
        shredded = castShredded(GenericVariant.fromJson("{\"a\": 27}"), schema);
        assertThat(reader.read(shredded, shredded.getBinary(schema.topLevelMetadataIdx)))
                .isEqualTo(GenericRow.of(27, null));
    }
}
