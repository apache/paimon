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

package org.apache.paimon.hive.mapred;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PaimonOutputFormat}. */
public class PaimonOutputFormatTest {

    @Test
    public void buildsRowMatchingUserBugReport() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "name", DataTypes.STRING()),
                                new DataField(2, "salary", DataTypes.DOUBLE()),
                                new DataField(3, "department", DataTypes.STRING()),
                                new DataField(4, "dt", DataTypes.STRING().notNull())),
                        4,
                        Collections.singletonList("dt"),
                        Arrays.asList("id", "dt"),
                        Collections.emptyMap(),
                        "");

        Path path = new Path("/wh/test_paimon2/dt=2026/file");

        GenericRow row = PaimonOutputFormat.buildStaticPartitionRow(path, schema);
        assertThat(row).isNotNull();
        assertThat(row.getFieldCount()).isEqualTo(1);
        assertThat(row.getString(0)).isEqualTo(BinaryString.fromString("2026"));
    }

    @Test
    public void buildRowConvertsTypedPartitionValues() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "v", DataTypes.INT()),
                                new DataField(1, "region", DataTypes.STRING().notNull()),
                                new DataField(2, "year", DataTypes.INT().notNull())),
                        2,
                        Arrays.asList("region", "year"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        Path path = new Path("/wh/t/region=us/year=2026/file");

        GenericRow row = PaimonOutputFormat.buildStaticPartitionRow(path, schema);
        assertThat(row).isNotNull();
        assertThat(row.getString(0)).isEqualTo(BinaryString.fromString("us"));
        assertThat(row.getInt(1)).isEqualTo(2026);
    }

    @Test
    public void buildRowReturnsNullForUnpartitionedTable() {
        GenericRow row =
                PaimonOutputFormat.buildStaticPartitionRow(
                        new Path("/wh/t/file"), singleFieldSchema());
        assertThat(row).isNull();
    }

    @Test
    public void buildRowReturnsNullWhenPathHasNoPartitionSegments() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "v", DataTypes.INT()),
                                new DataField(1, "dt", DataTypes.STRING().notNull())),
                        1,
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        GenericRow row =
                PaimonOutputFormat.buildStaticPartitionRow(new Path("/wh/t/_tmp/file"), schema);
        assertThat(row).isNull();
    }

    @Test
    public void buildRowFailsWhenPartitionKeysNotAtSchemaTail() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "id", DataTypes.INT().notNull()),
                                new DataField(1, "dt", DataTypes.STRING().notNull()),
                                new DataField(2, "name", DataTypes.STRING())),
                        2,
                        Collections.singletonList("dt"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        Path path = new Path("/wh/t/dt=2026/file");

        assertThatThrownBy(() -> PaimonOutputFormat.buildStaticPartitionRow(path, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("trailing columns")
                .hasMessageContaining("'dt'");
    }

    @Test
    public void buildRowFailsOnMixedStaticAndDynamicPartition() {
        TableSchema schema =
                new TableSchema(
                        0,
                        Arrays.asList(
                                new DataField(0, "v", DataTypes.INT()),
                                new DataField(1, "region", DataTypes.STRING().notNull()),
                                new DataField(2, "year", DataTypes.INT().notNull())),
                        2,
                        Arrays.asList("region", "year"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        "");

        Path path = new Path("/wh/t/region=us/file");

        assertThatThrownBy(() -> PaimonOutputFormat.buildStaticPartitionRow(path, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Mixed static and dynamic partition");
    }

    private static TableSchema singleFieldSchema() {
        return new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "v", DataTypes.INT())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyMap(),
                "");
    }
}
