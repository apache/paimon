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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for {@link FlinkExternalCatalog}.
 */
public class FlinkExternalCatalogTest {

    private static String defaultDatabase = "default";
    private static String database1 = "d1";

    private static String database2 = "d2";

    private static ObjectPath paimonTable1 = new ObjectPath(defaultDatabase, "t1");

    private static ObjectPath paimonTable2 = new ObjectPath(database1, "t2");

    private static ObjectPath paimonTable3 = new ObjectPath(database1, "t3");
    private static ObjectPath externalTable1 = new ObjectPath(defaultDatabase, "et1");

    private static ObjectPath externalTable2 = new ObjectPath(database1, "et2");
    private Catalog catalog;

    @TempDir
    public static java.nio.file.Path temporaryFolder;

    private ResolvedSchema createSchema() {
        return new ResolvedSchema(
                Arrays.asList(
                        Column.physical("first", DataTypes.STRING()),
                        Column.physical("second", DataTypes.INT()),
                        Column.physical("third", DataTypes.STRING()),
                        Column.physical(
                                "four",
                                DataTypes.ROW(
                                        DataTypes.FIELD("f1", DataTypes.STRING()),
                                        DataTypes.FIELD("f2", DataTypes.INT()),
                                        DataTypes.FIELD(
                                                "f3",
                                                DataTypes.MAP(
                                                        DataTypes.STRING(), DataTypes.INT()))))),
                Collections.emptyList(),
                null);
    }

    private CatalogTable createTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        String path = new File(temporaryFolder.toFile(), UUID.randomUUID().toString()).toString();
        Options conf = new Options();
        conf.setString("warehouse", path);
        catalog =
                FlinkExternalCatalogFactory.createCatalog(
                        FlinkCatalogTest.class.getClassLoader(),
                        conf.toMap(),
                        "test-external-catalog");
    }

    private CatalogTable createAnotherTable(Map<String, String> options) {
        ResolvedSchema resolvedSchema = this.createSchema();
        CatalogTable origin =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(resolvedSchema).build(),
                        "test comment",
                        Collections.emptyList(),
                        options);
        return new ResolvedCatalogTable(origin, resolvedSchema);
    }

    @Test
    public void testCreateDataBase() throws Exception {
        catalog.createDatabase(database1, null, true);
        assertThatThrownBy(() -> catalog.createDatabase(database1, null, false))
                .isInstanceOf(DatabaseAlreadyExistException.class)
                .hasMessageContaining("already exists in Catalog test-external-catalog");
        catalog.createDatabase(database2, null, false);
    }

    @Test
    public void testListDatabases() throws Exception {
        assertThat(catalog.listDatabases().size()).isEqualTo(2);
        catalog.createDatabase(database1, null, true);
        catalog.createDatabase(database2, null, false);
        assertThat(catalog.listDatabases().size()).isEqualTo(4);
    }

    @Test
    public void dropDatabase() throws Exception {
        catalog.createDatabase(database1, null, true);
        assertThat(catalog.listDatabases().size()).isEqualTo(3);
        catalog.dropDatabase(database1, true);
        assertThat(catalog.listDatabases().size()).isEqualTo(2);
    }

    @Test
    public void createTable() throws Exception {
        HashMap<String, String> paimonOptions = new HashMap<>();
        CatalogTable table = createTable(paimonOptions);
        HashMap<String, String> externalTableOptions = new HashMap<>();
        externalTableOptions.put("connector","kafka");
        CatalogTable externalTable = createTable(externalTableOptions);
        catalog.createTable(paimonTable1, table, true);
        assertThat(catalog.listTables(defaultDatabase).size()).isEqualTo(1);
        catalog.createTable(externalTable1, table, true);
        assertThat(catalog.listTables(defaultDatabase).size()).isEqualTo(2);
        assertThatThrownBy(() -> catalog.createTable(paimonTable2, table, true))
                .isInstanceOf(DatabaseNotExistException.class)
                .hasMessageContaining("Database d1 does not exist in Catalog test-external-catalog");
        catalog.createDatabase(database1, null, true);
        catalog.createTable(paimonTable2, table, true);
        catalog.createTable(paimonTable3, table, true);
        catalog.createTable(externalTable2, externalTable, true);
        assertThat(catalog.listTables(database1).size()).isEqualTo(3);
        assertThatThrownBy(() -> catalog.createTable(externalTable2, externalTable, false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("Table (or view) d1.et2 already exists in Catalog test-external-catalog");
    }

    @Test
    public void dropTable() throws Exception {
        HashMap<String, String> externalTableOptions = new HashMap<>();
        externalTableOptions.put("connector","kafka");
        CatalogTable externalTable = createTable(externalTableOptions);
        catalog.createTable(externalTable1, externalTable, true);
        assertThatThrownBy(() -> catalog.createTable(externalTable1, externalTable, false))
                .isInstanceOf(TableAlreadyExistException.class)
                .hasMessageContaining("Table (or view) default.et1 already exists in Catalog test-external-catalog");
        catalog.dropTable(externalTable1,true);
        catalog.createTable(externalTable1, externalTable, false);
    }

    private void checkEquals(ObjectPath path, CatalogTable t1, CatalogTable t2) {
        Path tablePath =
                ((AbstractCatalog) ((FlinkExternalCatalog) catalog).catalog())
                        .getDataTableLocation(FlinkCatalog.toIdentifier(path));
        Map<String, String> options = new HashMap<>(t1.getOptions());
        options.put("path", tablePath.toString());
        t1 = ((ResolvedCatalogTable) t1).copy(options);
        checkEquals(t1, t2);
    }

    private static void checkEquals(CatalogTable t1, CatalogTable t2) {
        assertThat(t2.getTableKind()).isEqualTo(t1.getTableKind());
        assertThat(t2.getSchema()).isEqualTo(t1.getSchema());
        assertThat(t2.getComment()).isEqualTo(t1.getComment());
        assertThat(t2.getPartitionKeys()).isEqualTo(t1.getPartitionKeys());
        assertThat(t2.isPartitioned()).isEqualTo(t1.isPartitioned());
        assertThat(t2.getOptions()).isEqualTo(t1.getOptions());
    }

    @Test
    public void testAlterTable() throws Exception {
        HashMap<String, String> options = new HashMap<>();
        CatalogTable table = this.createTable(options);
        catalog.createTable(paimonTable1, table, false);
        checkEquals(paimonTable1,table, (CatalogTable) catalog.getTable(paimonTable1));
        CatalogTable newTable = this.createAnotherTable(options);
        catalog.alterTable(paimonTable1, newTable, false);
        assertThat(catalog.getTable(paimonTable1)).isNotEqualTo(table);
        checkEquals(paimonTable1,newTable, (CatalogTable) catalog.getTable(paimonTable1));
        catalog.dropTable(paimonTable1, false);
    }

    @Test
    public void testGetTable() throws Exception{
        HashMap<String, String> externalTableOptions = new HashMap<>();
        externalTableOptions.put("connector","kafka");
        CatalogTable table = this.createTable(externalTableOptions);
        catalog.createTable(externalTable1, table, false);
        checkEquals(externalTable1,((CatalogTable) catalog.getTable(externalTable1)),  table);
        HashMap<String, String> options = new HashMap<>();
        externalTableOptions.put("connector","kafka");
        CatalogTable paimonTable = this.createTable(options);
        catalog.createTable(paimonTable1, paimonTable, false);
        checkEquals(paimonTable1,table, (CatalogTable) catalog.getTable(paimonTable1));
    }
}
