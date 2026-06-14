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

package org.apache.paimon.catalog;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CatalogUtils}. */
public class CatalogUtilsTest {

    @Test
    public void testValidateBlobFormatTable() {
        assertThatNoException()
                .isThrownBy(
                        () ->
                                CatalogUtils.validateCreateTable(
                                        Schema.newBuilder()
                                                .column("payload", DataTypes.BLOB())
                                                .column("ds", DataTypes.INT())
                                                .partitionKeys("ds")
                                                .options(blobFormatTableOptions())
                                                .build(),
                                        false));

        assertThatThrownBy(
                        () ->
                                CatalogUtils.validateCreateTable(
                                        Schema.newBuilder()
                                                .column("payload", DataTypes.BLOB())
                                                .column("id", DataTypes.INT())
                                                .column("ds", DataTypes.INT())
                                                .partitionKeys("ds")
                                                .options(blobFormatTableOptions())
                                                .build(),
                                        false))
                .hasMessageContaining("only supports one non-partition field");

        assertThatThrownBy(
                        () ->
                                CatalogUtils.validateCreateTable(
                                        Schema.newBuilder()
                                                .column("payload", DataTypes.BYTES())
                                                .column("ds", DataTypes.INT())
                                                .partitionKeys("ds")
                                                .options(blobFormatTableOptions())
                                                .build(),
                                        false))
                .hasMessageContaining("only supports BLOB type as non-partition field");

        assertThatThrownBy(
                        () ->
                                CatalogUtils.validateCreateTable(
                                        Schema.newBuilder()
                                                .column("payload", DataTypes.BLOB())
                                                .partitionKeys("payload")
                                                .options(blobFormatTableOptions())
                                                .build(),
                                        false))
                .hasMessageContaining("only supports one non-partition field");
    }

    private Map<String, String> blobFormatTableOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("type", "format-table");
        options.put("file.format", "blob");
        return options;
    }
}
