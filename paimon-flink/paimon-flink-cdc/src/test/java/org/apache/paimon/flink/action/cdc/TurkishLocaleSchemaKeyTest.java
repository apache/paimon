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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * The CDC action layer case-folds identifiers and option values before matching them. {@link
 * CdcActionCommonUtils#buildPaimonSchema} does it through separate helpers for field names and for
 * key lists, and {@link TypeMapping#parse} does it for `--type-mapping` values. Under a Turkish
 * default locale 'I' lowercases to a dotless glyph, so every one of those conversions has to pin
 * {@link Locale#ROOT}: otherwise a key list holds a name no field has, and an option spelled in
 * upper case stops matching any mode.
 */
class TurkishLocaleSchemaKeyTest {

    private Locale original;

    @BeforeEach
    void setUp() {
        original = Locale.getDefault();
        Locale.setDefault(new Locale("tr", "TR"));
    }

    @AfterEach
    void tearDown() {
        Locale.setDefault(original);
    }

    @Test
    void primaryKeyInferredFromSourceSchemaNamesTheField() {
        Schema source = Schema.newBuilder().column("ID", DataTypes.INT()).primaryKey("ID").build();

        Schema result = build(Collections.emptyList(), Collections.emptyList(), source, true, true);

        assertThat(result.fields().get(0).name()).isEqualTo("id");
        assertThat(result.primaryKeys()).containsExactly("id");
    }

    @Test
    void specifiedPrimaryKeyNamesTheField() {
        Schema source = Schema.newBuilder().column("ID", DataTypes.INT()).build();

        Schema result =
                build(
                        Collections.emptyList(),
                        Collections.singletonList("ID"),
                        source,
                        false,
                        false);

        assertThat(result.primaryKeys()).containsExactly("id");
    }

    @Test
    void specifiedPrimaryKeyPassesStrictChecking() {
        Schema source = Schema.newBuilder().column("ID", DataTypes.INT()).build();

        assertThatCode(
                        () ->
                                build(
                                        Collections.emptyList(),
                                        Collections.singletonList("ID"),
                                        source,
                                        true,
                                        false))
                .doesNotThrowAnyException();
    }

    @Test
    void specifiedPartitionKeySurvivesNonStrictChecking() {
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("CITY", DataTypes.STRING())
                        .build();

        Schema result =
                build(
                        Collections.singletonList("CITY"),
                        Collections.emptyList(),
                        source,
                        false,
                        false);

        assertThat(result.partitionKeys()).containsExactly("city");
    }

    @Test
    void upperCaseTypeMappingOptionStillMatchesItsMode() {
        TypeMapping mapping = TypeMapping.parse(new String[] {"TINYINT1-NOT-BOOL"});

        assertThat(mapping.containsMode(TypeMapping.TypeMappingMode.TINYINT1_NOT_BOOL)).isTrue();
    }

    private static Schema build(
            List<String> specifiedPartitionKeys,
            List<String> specifiedPrimaryKeys,
            Schema sourceSchema,
            boolean strictlyCheckSpecified,
            boolean syncPKeysFromSourceSchema) {
        return CdcActionCommonUtils.buildPaimonSchema(
                "T",
                specifiedPartitionKeys,
                specifiedPrimaryKeys,
                Collections.emptyList(),
                Collections.emptyMap(),
                sourceSchema,
                new CdcMetadataConverter[0],
                false,
                strictlyCheckSpecified,
                false,
                syncPKeysFromSourceSchema);
    }
}
