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

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.schema.SchemaValidation.validateTableSchema;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for BLOB_REF-specific schema validation. */
public class BlobRefSchemaValidationTest {

    @Test
    public void testNestedBlobRefTableSchema() {
        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        options.put(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        options.put(BUCKET.key(), String.valueOf(-1));

        List<DataField> fields =
                Arrays.asList(
                        new DataField(0, "f0", DataTypes.INT()),
                        new DataField(
                                1,
                                "f1",
                                DataTypes.ROW(DataTypes.FIELD(2, "nested", DataTypes.BLOB_REF()))));

        assertThatThrownBy(
                        () ->
                                validateTableSchema(
                                        new TableSchema(
                                                1,
                                                fields,
                                                10,
                                                emptyList(),
                                                emptyList(),
                                                options,
                                                "")))
                .hasMessage(
                        "Nested BLOB_REF type is not supported. Field 'f1' contains a nested BLOB_REF.");
    }
}
