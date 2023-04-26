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

package org.apache.paimon.flink;

import org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.WatermarkSpec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.DescriptorProperties.DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.METADATA;
import static org.apache.flink.table.descriptors.DescriptorProperties.NAME;
import static org.apache.flink.table.descriptors.DescriptorProperties.VIRTUAL;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.paimon.flink.utils.FlinkCatalogPropertiesUtil.compoundKey;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlinkCatalogPropertiesUtil}. */
public class FlinkCatalogPropertiesUtilTest {

    @Test
    public void testSerDeNonPhysicalColumns() {
        Map<String, Integer> indexMap = new HashMap<>();
        indexMap.put("comp", 2);
        indexMap.put("meta1", 3);
        indexMap.put("meta2", 5);
        List<TableColumn> columns = new ArrayList<>();
        columns.add(TableColumn.computed("comp", DataTypes.INT(), "`k` * 2"));
        columns.add(TableColumn.metadata("meta1", DataTypes.VARCHAR(10)));
        columns.add(TableColumn.metadata("meta2", DataTypes.BIGINT().notNull(), "price", true));

        // validate serialization
        Map<String, String> serialized =
                FlinkCatalogPropertiesUtil.serializeNonPhysicalColumns(indexMap, columns);

        Map<String, String> expected = new HashMap<>();
        expected.put(compoundKey(SCHEMA, 2, NAME), "comp");
        expected.put(compoundKey(SCHEMA, 2, DATA_TYPE), "INT");
        expected.put(compoundKey(SCHEMA, 2, EXPR), "`k` * 2");

        expected.put(compoundKey(SCHEMA, 3, NAME), "meta1");
        expected.put(compoundKey(SCHEMA, 3, DATA_TYPE), "VARCHAR(10)");
        expected.put(compoundKey(SCHEMA, 3, METADATA), "meta1");
        expected.put(compoundKey(SCHEMA, 3, VIRTUAL), "false");

        expected.put(compoundKey(SCHEMA, 5, NAME), "meta2");
        expected.put(compoundKey(SCHEMA, 5, DATA_TYPE), "BIGINT NOT NULL");
        expected.put(compoundKey(SCHEMA, 5, METADATA), "price");
        expected.put(compoundKey(SCHEMA, 5, VIRTUAL), "true");

        assertThat(serialized).containsExactlyInAnyOrderEntriesOf(expected);

        // validate deserialization
        List<TableColumn> deserialized = new ArrayList<>();
        deserialized.add(FlinkCatalogPropertiesUtil.deserializeNonPhysicalColumn(serialized, 2));
        deserialized.add(FlinkCatalogPropertiesUtil.deserializeNonPhysicalColumn(serialized, 3));
        deserialized.add(FlinkCatalogPropertiesUtil.deserializeNonPhysicalColumn(serialized, 5));

        assertThat(deserialized).isEqualTo(columns);

        // validate that
    }

    @Test
    public void testSerDeWatermarkSpec() {
        WatermarkSpec watermarkSpec =
                new WatermarkSpec(
                        "test_time",
                        "`test_time` - INTERVAL '0.001' SECOND",
                        DataTypes.TIMESTAMP(3));

        // validate serialization
        Map<String, String> serialized =
                FlinkCatalogPropertiesUtil.serializeWatermarkSpec(watermarkSpec);

        Map<String, String> expected = new HashMap<>();
        String watermarkPrefix = compoundKey(SCHEMA, WATERMARK, 0);
        expected.put(compoundKey(watermarkPrefix, WATERMARK_ROWTIME), "test_time");
        expected.put(
                compoundKey(watermarkPrefix, WATERMARK_STRATEGY_EXPR),
                "`test_time` - INTERVAL '0.001' SECOND");
        expected.put(compoundKey(watermarkPrefix, WATERMARK_STRATEGY_DATA_TYPE), "TIMESTAMP(3)");

        assertThat(serialized).containsExactlyInAnyOrderEntriesOf(expected);

        // validate serialization
        WatermarkSpec deserialized =
                FlinkCatalogPropertiesUtil.deserializeWatermarkSpec(serialized);
        assertThat(deserialized).isEqualTo(watermarkSpec);
    }

    @Test
    public void testNonPhysicalColumnsCount() {
        Map<String, String> oldStyleOptions = new HashMap<>();
        // physical
        oldStyleOptions.put(compoundKey(SCHEMA, 0, NAME), "phy1");
        oldStyleOptions.put(compoundKey(SCHEMA, 0, DATA_TYPE), "INT");
        oldStyleOptions.put(compoundKey(SCHEMA, 1, NAME), "phy2");
        oldStyleOptions.put(compoundKey(SCHEMA, 1, DATA_TYPE), "INT NOT NULL");

        // non-physical
        oldStyleOptions.put(compoundKey(SCHEMA, 2, NAME), "comp");
        oldStyleOptions.put(compoundKey(SCHEMA, 2, DATA_TYPE), "INT");
        oldStyleOptions.put(compoundKey(SCHEMA, 2, EXPR), "`k` * 2");

        oldStyleOptions.put(compoundKey(SCHEMA, 3, NAME), "meta1");
        oldStyleOptions.put(compoundKey(SCHEMA, 3, DATA_TYPE), "VARCHAR(10)");
        oldStyleOptions.put(compoundKey(SCHEMA, 3, METADATA), "meta1");
        oldStyleOptions.put(compoundKey(SCHEMA, 3, VIRTUAL), "false");

        oldStyleOptions.put(compoundKey(SCHEMA, 4, NAME), "meta2");
        oldStyleOptions.put(compoundKey(SCHEMA, 4, DATA_TYPE), "BIGINT NOT NULL");
        oldStyleOptions.put(compoundKey(SCHEMA, 4, METADATA), "price");
        oldStyleOptions.put(compoundKey(SCHEMA, 4, VIRTUAL), "true");

        // other options
        oldStyleOptions.put("schema.unknown.name", "test");

        assertThat(
                        FlinkCatalogPropertiesUtil.nonPhysicalColumnsCount(
                                oldStyleOptions, Arrays.asList("phy1", "phy2")))
                .isEqualTo(3);
    }
}
