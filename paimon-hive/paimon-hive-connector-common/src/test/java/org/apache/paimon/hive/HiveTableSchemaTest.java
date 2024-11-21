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

package org.apache.paimon.hive;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link HiveSchema}.
 */
public class HiveTableSchemaTest {

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new DataField(0, "a", DataTypes.INT(), "first comment"),
                            new DataField(1, "b", DataTypes.STRING(), "second comment"),
                            new DataField(2, "c", DataTypes.DECIMAL(5, 3), "last comment")));

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    public void testExtractSchemaWithEmptyDDLAndNoPaimonTable() {
        // Extract schema with empty DDL and no paimon table
        Properties tableWithEmptyDDL = createTableWithEmptyDDL();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> HiveSchema.extract(null, tableWithEmptyDDL))
                .withMessage(
                        "Schema file not found in location "
                                + tempDir.toString()
                                + ". Please create table first.");
    }

    @Test
    public void testExtractSchemaWithEmptyDDLAndExistsPaimonTable() throws Exception {
        // create a paimon table
        createSchema();
        // Extract schema with empty DDL and exists paimon table
        Properties tableWithEmptyDDL = createTableWithEmptyDDL();

        HiveSchema schema = HiveSchema.extract(null, tableWithEmptyDDL);
        assertThat(schema.fieldNames()).isEqualTo(Arrays.asList("a", "b", "c"));
        assertThat(schema.fieldTypes())
                .isEqualTo(
                        Arrays.asList(
                                DataTypes.INT(), DataTypes.STRING(), DataTypes.DECIMAL(5, 3)));
        assertThat(schema.fieldComments())
                .isEqualTo(Arrays.asList("first comment", "second comment", "last comment"));
    }

    @Test
    public void testExtractSchemaWithExistsDDLAndNoPaimonTable() {
        // Extract schema with exists DDL and no paimon table
        Properties tableWithExistsDDL = createTableWithExistsDDL();

        HiveSchema schema = HiveSchema.extract(null, tableWithExistsDDL);
        assertThat(schema.fieldNames()).isEqualTo(Arrays.asList("a", "b", "c"));
        assertThat(schema.fieldTypes())
                .isEqualTo(
                        Arrays.asList(
                                DataTypes.INT(), DataTypes.STRING(), DataTypes.DECIMAL(5, 3)));
        assertThat(schema.fieldComments())
                .isEqualTo(Arrays.asList("col1 comment", "col2 comment", "col3 comment"));
    }

    @Test
    public void testExtractSchemaWithExistsDDLAndExistsPaimonTable() throws Exception {
        // create a paimon table
        createSchema();
        // Extract schema with exists DDL and exists paimon table
        Properties tableWithExistsDDL = createTableWithExistsDDL();

        HiveSchema schema = HiveSchema.extract(null, tableWithExistsDDL);
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
        properties.setProperty("columns.comments", "\0\0");
        properties.setProperty("location", tempDir.toString());

        String expected =
                "Hive DDL and paimon schema mismatched! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "Mismatched fields are:\n"
                        + "Field #2\n"
                        + "Hive DDL          : mismatched string\n"
                        + "Paimon Schema: b string\n"
                        + "--------------------\n"
                        + "Field #3\n"
                        + "Hive DDL          : c decimal(6,3)\n"
                        + "Paimon Schema: c decimal(5,3)";

        assertThatThrownBy(() -> HiveSchema.extract(null, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expected);
    }


    @Test
    public void testSubsetColumnNameAndType() throws Exception {
        createSchema();
        Properties properties = new Properties();
        List<String> columns = Arrays.asList("a","b");
        properties.setProperty("columns", String.join(",",columns));
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(6, 3).getTypeName())));
        properties.setProperty("columns.comments", "\0\0");
        properties.setProperty("location", tempDir.toString());
        List<String> fields = HiveSchema.extract(null, properties).fieldNames();
        assertThat(fields).isEqualTo(columns);
    }

    @Test
    public void testSupersetColumnNameAndType() throws Exception {
        createSchema();
        Properties properties = new Properties();
        properties.setProperty("columns", "a,b,c,d");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.decimalTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(6, 3).getTypeName())));
        properties.setProperty("columns.comments", "\0\0");
        properties.setProperty("location", tempDir.toString());
        String expected =
                "Hive DDL is a superset of paimon schema! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "There are 4 fields in Hive DDL: a, b, c, d\n"
                        + "There are 3 fields in Paimon schema: a, b, c\n";
        assertThatThrownBy(() -> HiveSchema.extract(null, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expected);
    }


    @Test
    public void testTooFewColumns() throws Exception {
        createSchema();

        Properties properties = new Properties();
        properties.setProperty("columns", "a");
        properties.setProperty("columns.types", TypeInfoFactory.intTypeInfo.getTypeName());
        properties.setProperty("location", tempDir.toString());
        properties.setProperty("columns.comments", "");

        String expected =
                "Hive DDL and paimon schema mismatched! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "There are 1 fields in Hive DDL: a\n"
                        + "There are 3 fields in Paimon schema: a, b, c";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> HiveSchema.extract(null, properties))
                .withMessageContaining(expected);
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
        properties.setProperty("columns.comments", "\0\0\0\0");
        properties.setProperty("location", tempDir.toString());

        String expected =
                "Hive DDL and paimon schema mismatched! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "There are 5 fields in Hive DDL: a, b, c, d, e\n"
                        + "There are 3 fields in Paimon schema: a, b, c";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> HiveSchema.extract(null, properties))
                .withMessageContaining(expected);
    }

    private void createSchema() throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()))
                .createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.emptyList(),
                                Collections.emptyList(),
                                new HashMap<>(),
                                ""));
    }

    @Test
    public void testMismatchedPartitionKeyAndType() throws Exception {
        createSchemaWithPartition();

        Properties properties = new Properties();
        properties.setProperty("partition_columns", "a/mismatched");
        properties.setProperty(
                "partition_columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.longTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName())));
        properties.setProperty("columns", "c");
        properties.setProperty(
                "columns.types", TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName());
        properties.setProperty("columns.comments", "");
        properties.setProperty("location", tempDir.toString());

        String expected =
                String.join(
                        "\n",
                        "Hive DDL and paimon schema mismatched! "
                                + "It is recommended not to write any column definition "
                                + "as Paimon external table can read schema from the specified location.\n"
                                + "Mismatched partition keys are:\n"
                                + "Partition Key #1\n"
                                + "Hive DDL          : a bigint\n"
                                + "Paimon Schema: a int\n"
                                + "--------------------\n"
                                + "Partition Key #2\n"
                                + "Hive DDL          : mismatched string\n"
                                + "Paimon Schema: b string");
        assertThatThrownBy(() -> HiveSchema.extract(null, properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(expected);
    }

    @Test
    public void testTooFewPartitionKeys() throws Exception {
        createSchemaWithPartition();

        Properties properties = new Properties();
        properties.setProperty("partition_columns", "a");
        properties.setProperty(
                "partition_columns.types", TypeInfoFactory.intTypeInfo.getTypeName());
        properties.setProperty("columns", "c");
        properties.setProperty(
                "columns.types", TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName());
        properties.setProperty("columns.comments", "");
        properties.setProperty("location", tempDir.toString());

        String expected =
                "Hive DDL and paimon schema mismatched! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "There are 1 partition keys in Hive DDL: a\n"
                        + "There are 2 partition keys in Paimon schema: a, b";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> HiveSchema.extract(null, properties))
                .withMessageContaining(expected);
    }

    @Test
    public void testTooManyPartitionKeys() throws Exception {
        createSchemaWithPartition();

        Properties properties = new Properties();
        properties.setProperty("partition_columns", "a/b/d");
        properties.setProperty(
                "partition_columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.intTypeInfo.getTypeName())));
        properties.setProperty("columns", "c");
        properties.setProperty(
                "columns.types", TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName());
        properties.setProperty("columns.comments", "");
        properties.setProperty("location", tempDir.toString());

        String expected =
                "Hive DDL and paimon schema mismatched! "
                        + "It is recommended not to write any column definition "
                        + "as Paimon external table can read schema from the specified location.\n"
                        + "There are 3 partition keys in Hive DDL: a, b, d\n"
                        + "There are 2 partition keys in Paimon schema: a, b";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> HiveSchema.extract(null, properties))
                .withMessageContaining(expected);
    }

    private void createSchemaWithPartition() throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(tempDir.toString()))
                .createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Arrays.asList("a", "b"),
                                Arrays.asList("a", "b", "c"),
                                new HashMap<>(),
                                ""));
    }

    private Properties createTableWithEmptyDDL() {
        String tableName = "empty_ddl_test_table";
        Properties properties = new Properties();
        properties.setProperty("name", tableName);
        properties.setProperty("columns", "");
        properties.setProperty("columns.types", "");
        properties.setProperty("location", tempDir.toString());
        return properties;
    }

    private Properties createTableWithExistsDDL() {
        String tableName = "test_table";
        Properties properties = new Properties();
        properties.setProperty("name", tableName);
        properties.setProperty("columns", "a,b,c");
        properties.setProperty(
                "columns.types",
                String.join(
                        ":",
                        Arrays.asList(
                                TypeInfoFactory.intTypeInfo.getTypeName(),
                                TypeInfoFactory.stringTypeInfo.getTypeName(),
                                TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName())));
        properties.setProperty("columns.comments", "col1 comment\0col2 comment\0col3 comment");
        properties.setProperty("location", tempDir.toString());
        return properties;
    }

    @Test
    public void testReadHiveSchemaFromProperties() throws Exception {
        createSchema();
        // cache the TableSchema to properties
        Properties properties = new Properties();
        properties.put(hive_metastoreConstants.META_TABLE_LOCATION, tempDir.toString());

        HiveSchema hiveSchema = HiveSchema.extract(null, properties);

        List<DataField> dataFields = hiveSchema.fields();
        String dataFieldStr = JsonSerdeUtil.toJson(dataFields);

        List<DataField> dataFieldsDeserialized =
                JsonSerdeUtil.fromJson(dataFieldStr, new TypeReference<List<DataField>>() {
                });
        HiveSchema newHiveSchema = new HiveSchema(new RowType(dataFieldsDeserialized));
        assertThat(newHiveSchema).usingRecursiveComparison().isEqualTo(hiveSchema);
    }
}
