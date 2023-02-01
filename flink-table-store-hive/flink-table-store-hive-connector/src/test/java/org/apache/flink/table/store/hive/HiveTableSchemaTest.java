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

import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.UpdateSchema;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.DataTypes;
import org.apache.flink.table.store.types.RowType;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link HiveSchema}. */
public class HiveTableSchemaTest {

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "a", DataTypes.INT(), "first comment"),
                            new DataField(1, "b", DataTypes.STRING(), "second comment"),
                            new DataField(2, "c", DataTypes.DECIMAL(5, 3), "last comment")));

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testExtractSchema() throws Exception {
        createSchema();

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
        properties.setProperty("location", tempDir.toString());

        HiveSchema schema = HiveSchema.extract(null, properties);
        assertThat(schema.fieldNames()).isEqualTo(Arrays.asList("a", "b", "c"));
        assertThat(schema.fieldTypes())
                .isEqualTo(
                        Arrays.asList(
                                DataTypes.INT(), DataTypes.STRING(), DataTypes.DECIMAL(5, 3)));
        assertThat(schema.fieldComments())
                .isEqualTo(Arrays.asList("first comment", "second comment", "last comment"));
    }

    @Test
    public void testExtractSchemaWithEmptyDDL() throws Exception {
        createSchema();

        Properties properties = new Properties();
        properties.setProperty("columns", "");
        properties.setProperty("columns.types", "");
        properties.setProperty("location", tempDir.toString());

        HiveSchema schema = HiveSchema.extract(null, properties);
        assertThat(schema.fieldNames()).isEqualTo(Arrays.asList("a", "b", "c"));
        assertThat(schema.fieldTypes())
                .isEqualTo(
                        Arrays.asList(
                                DataTypes.INT(), DataTypes.STRING(), DataTypes.DECIMAL(5, 3)));
        assertThat(schema.fieldComments())
                .isEqualTo(Arrays.asList("first comment", "second comment", "last comment"));
    }

    @Test
    public void testMismatchedColumnNameAndType() throws Exception {
        createSchema();

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
                        "Hive DDL and table store schema mismatched! "
                                + "It is recommended not to write any column definition "
                                + "as Flink table store external table can read schema from the specified location.",
                        "Mismatched fields are:",
                        "Field #1",
                        "Hive DDL          : mismatched string",
                        "Table Store Schema: b string",
                        "--------------------",
                        "Field #2",
                        "Hive DDL          : c decimal(6,3)",
                        "Table Store Schema: c decimal(5,3)");
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> HiveSchema.extract(null, properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testTooFewColumns() throws Exception {
        createSchema();

        Properties properties = new Properties();
        properties.setProperty("columns", "a");
        properties.setProperty("columns.types", TypeInfoFactory.intTypeInfo.getTypeName());
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and table store schema mismatched! "
                                + "It is recommended not to write any column definition "
                                + "as Flink table store external table can read schema from the specified location.",
                        "Mismatched fields are:",
                        "Field #1",
                        "Hive DDL          : null",
                        "Table Store Schema: b string",
                        "--------------------",
                        "Field #2",
                        "Hive DDL          : null",
                        "Table Store Schema: c decimal(5,3)");
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> HiveSchema.extract(null, properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    @Test
    public void testTooManyColumns() throws Exception {
        createSchema();

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
                        "Hive DDL and table store schema mismatched! "
                                + "It is recommended not to write any column definition "
                                + "as Flink table store external table can read schema from the specified location.",
                        "Mismatched fields are:",
                        "Field #3",
                        "Hive DDL          : d int",
                        "Table Store Schema: null",
                        "--------------------",
                        "Field #4",
                        "Hive DDL          : e string",
                        "Table Store Schema: null");
        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class, () -> HiveSchema.extract(null, properties));
        assertThat(exception).hasMessageContaining(expected);
    }

    private void createSchema() throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()))
                .commitNewVersion(
                        new UpdateSchema(
                                ROW_TYPE,
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
    }
}
