/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link HiveSchema}. */
public class HiveSchemaTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.DECIMAL(5, 3).getLogicalType()
                    },
                    new String[] {"a", "b", "c"});

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testExtractSchema() throws Exception {
        createSchema(Collections.singletonList("a"), Arrays.asList("a", "b"));

        Properties properties = new Properties();
        properties.setProperty("columns", "a,b,c");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName())));
        properties.setProperty("columns.comments", "first comment\0second comment\0last comment");
        properties.setProperty("partition_columns", "a");
        properties.setProperty("table-store.primary-keys", "a,b");
        properties.setProperty("table-store.bucket", "2");
        properties.setProperty("table-store.file.format", "avro");
        properties.setProperty("location", tempDir.toString());
        // useless table properties
        properties.setProperty("table-store.useless-option", "test");
        properties.setProperty("not.table-store.keys", "useless");

        HiveSchema schema = HiveSchema.extract(properties);
        assertThat(schema.fieldNames()).isEqualTo(Arrays.asList("a", "b", "c"));
        assertThat(schema.fieldTypes())
                .isEqualTo(
                        Arrays.asList(
                                DataTypes.INT().getLogicalType(),
                                DataTypes.STRING().getLogicalType(),
                                DataTypes.DECIMAL(5, 3).getLogicalType()));
        assertThat(schema.fieldComments())
                .isEqualTo(Arrays.asList("first comment", "second comment", "last comment"));
        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("bucket", "2");
        expectedOptions.put("file.format", "avro");
        assertThat(schema.tableStoreOptions()).isEqualTo(expectedOptions);
    }

    @Test
    public void testMismatchedColumnNameAndType() throws Exception {
        createSchema(Collections.emptyList(), Collections.emptyList());

        Properties properties = new Properties();
        properties.setProperty("columns", "a,mismatched,c");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(6, 3).getTypeName())));
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema mismatched! Mismatched fields are:",
                        "Field #1",
                        "Hive DDL          : mismatched string",
                        "Table Store Schema: b string",
                        "--------------------",
                        "Field #2",
                        "Hive DDL          : c decimal(6,3)",
                        "Table Store Schema: c decimal(5,3)");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> HiveSchema.extract(properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testTooFewColumns() throws Exception {
        createSchema(Collections.emptyList(), Collections.emptyList());

        Properties properties = new Properties();
        properties.setProperty("columns", "a");
        properties.setProperty("columns.types", TypeInfoFactory.intTypeInfo.getTypeName());
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema mismatched! Mismatched fields are:",
                        "Field #1",
                        "Hive DDL          : null",
                        "Table Store Schema: b string",
                        "--------------------",
                        "Field #2",
                        "Hive DDL          : null",
                        "Table Store Schema: c decimal(5,3)");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> HiveSchema.extract(properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testTooManyColumns() throws Exception {
        createSchema(Collections.emptyList(), Collections.emptyList());

        Properties properties = new Properties();
        properties.setProperty("columns", "a,b,c,d,e");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName(),
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName())));
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema mismatched! Mismatched fields are:",
                        "Field #3",
                        "Hive DDL          : d int",
                        "Table Store Schema: null",
                        "--------------------",
                        "Field #4",
                        "Hive DDL          : e string",
                        "Table Store Schema: null");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> HiveSchema.extract(properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testMismatchedPartitionKeys() throws Exception {
        createSchema(Arrays.asList("a", "b"), Collections.emptyList());

        Properties properties = new Properties();
        properties.setProperty("columns", "a,b,c");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName())));
        properties.setProperty("partition_columns", "a");
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema have different partition keys!",
                        "Hive DDL          : [a]",
                        "Table Store Schema: [a, b]");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> HiveSchema.extract(properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testMismatchedPrimaryKeys() throws Exception {
        createSchema(Collections.emptyList(), Arrays.asList("a", "b"));

        Properties properties = new Properties();
        properties.setProperty("columns", "a,b,c");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName())));
        properties.setProperty("table-store.primary-keys", "a");
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema have different primary keys!",
                        "Hive DDL          : [a]",
                        "Table Store Schema: [a, b]");
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> HiveSchema.extract(properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    private void createSchema(List<String> partitionKeys, List<String> primaryKeys)
            throws Exception {
        new SchemaManager(new Path(tempDir.toString()))
                .commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE, partitionKeys, primaryKeys, new HashMap<>(), ""));
    }
}
