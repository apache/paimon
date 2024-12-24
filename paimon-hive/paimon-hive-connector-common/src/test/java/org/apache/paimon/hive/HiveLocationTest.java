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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.annotation.Minio;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.s3.MinioTestContainer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for specify location. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class HiveLocationTest {
    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    @Minio private static MinioTestContainer minioTestContainer;

    public static final String HIVE_CONF = "/hive-conf";

    private HiveCatalog catalog;

    private IMetaStoreClient hmsClient;

    private String objectStorePath;

    private FileIO fileIO;

    private CatalogContext catalogContext;

    @Before
    public void before() throws IOException {
        objectStorePath = minioTestContainer.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();

        Options options = new Options();
        options.set(CatalogOptions.WAREHOUSE, objectStorePath);
        options.set(CatalogOptions.METASTORE, "hive");
        options.set(CatalogOptions.URI, "");
        options.set(CatalogOptions.LOCK_ENABLED, false);
        options.set(
                HiveCatalogOptions.HIVE_CONF_DIR,
                hiveShell.getBaseDir().getRoot().getPath() + HIVE_CONF);
        options.set(HiveCatalogOptions.LOCATION_IN_PROPERTIES, true);

        for (Map.Entry<String, String> stringStringEntry :
                minioTestContainer.getS3ConfigOptions().entrySet()) {
            options.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }

        // create CatalogContext using the options
        catalogContext = CatalogContext.create(options);

        Path warehouse = new Path(objectStorePath);
        fileIO = getFileIO(catalogContext, warehouse);
        fileIO.mkdirs(warehouse);

        HiveCatalogFactory hiveCatalogFactory = new HiveCatalogFactory();
        catalog = (HiveCatalog) hiveCatalogFactory.create(catalogContext);

        hmsClient = catalog.getHmsClient();

        String setTemplate = "SET paimon.%s=%s";
        minioTestContainer
                .getS3ConfigOptions()
                .forEach(
                        (k, v) -> {
                            hiveShell.execute(String.format(setTemplate, k, v));
                        });
    }

    private static FileIO getFileIO(CatalogContext catalogContext, Path warehouse) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(warehouse, catalogContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return fileIO;
    }

    @After
    public void after() throws Exception {
        catalog.close();
        hiveShell.execute("DROP DATABASE IF EXISTS database1 CASCADE");
    }

    @Test
    public void testCatalogDBLocation() throws Exception {

        Set<String> dbs = Sets.newHashSet("db1", "db2", "db3", "db4", "db5");
        List<Path> paths = new ArrayList<>();
        for (String db : dbs) {
            catalog.createDatabase(db, true);
            assertThat(hmsClient.getDatabase(db)).isNotNull();

            Path actual = catalog.newDatabasePath(db);
            Path expected = new Path(this.objectStorePath + "/" + db + ".db");
            assertThat(fileIO.exists(expected)).isTrue();
            assertThat(actual).isEqualTo(expected);

            paths.add(expected);
        }

        HashSet<String> dbsExpected = Sets.newHashSet("db1", "db2", "db3", "db4", "db5", "default");
        assertThat(Sets.newHashSet(catalog.listDatabases())).isEqualTo(dbsExpected);

        for (String db : dbs) {
            catalog.dropDatabase(db, false, true);
        }

        for (Path p : paths) {
            assertThat(fileIO.exists(p)).isFalse();
        }

        assertThat(Sets.newHashSet(catalog.listDatabases())).isEqualTo(Sets.newHashSet("default"));
    }

    @Test
    public void testCatalogTableLocation() throws Exception {

        String db = "db";
        String table = "table";

        catalog.createDatabase(db, true);

        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"aaa"});
        Identifier tableIdentifier = Identifier.create(db, table);

        // create table
        catalog.createTable(
                tableIdentifier,
                new Schema(
                        rowType.getFields(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        new HashMap<>(),
                        ""),
                false);

        Table hmsClientTablea =
                hmsClient.getTable(
                        tableIdentifier.getDatabaseName(), tableIdentifier.getObjectName());
        String location =
                hmsClientTablea.getParameters().get(LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY);
        String expected = this.objectStorePath + "/" + db + ".db" + "/" + table;
        assertThat(fileIO.exists(new Path(expected))).isTrue();
        assertThat(location).isEqualTo(expected);
    }

    @Test
    public void testExternTableLocation() throws Exception {

        String path = minioTestContainer.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();

        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);

        for (Map.Entry<String, String> stringStringEntry :
                minioTestContainer.getS3ConfigOptions().entrySet()) {
            conf.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }

        RowType rowType = RowType.of(new DataType[] {DataTypes.INT()}, new String[] {"aaa"});
        // create table with location field
        assertThatThrownBy(
                        () ->
                                createTableWithStorageLocation(
                                        path, rowType, "test_extern_table", conf, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No FileSystem for scheme: s3");

        // create table with location in table properties
        Set<String> tableForTest = Sets.newHashSet("test_extern_table1", "hive_inner_table1");

        int i = 0;
        for (String table : tableForTest) {
            createTableWithPropertiesLocation(path, rowType, table, conf, ++i % 2 == 0);
        }

        Set<String> tableInHive = Sets.newHashSet(hiveShell.executeQuery("show tables"));
        assertThat(tableInHive).isEqualTo(tableForTest);
    }

    @Test
    public void testRWIT() {
        String dbName = "database1";
        String createDbStr = String.format("create database %s ;", dbName);
        String useDbStr = String.format("use %s ;", dbName);

        hiveShell.execute(createDbStr);
        hiveShell.execute(useDbStr);

        String[][] params =
                new String[][] {
                    {"table1", objectStorePath},
                    {"table2", hiveShell.getBaseDir().getRoot().getAbsolutePath()},
                };
        for (String[] param : params) {
            String tableName = param[0];
            String warehouse = param[1];
            boolean locationInProperties = true;

            Identifier identifier = Identifier.create(dbName, tableName);
            String location =
                    CatalogUtils.newTableLocation(warehouse, identifier).toUri().toString();

            String createTableSqlStr =
                    getCreateTableSqlStr(tableName, location, locationInProperties);
            testRWinHive(createTableSqlStr, location, tableName);
        }
        String associationSql = "select a,b from table1 union all select a,b from table2";
        List<String> result = hiveShell.executeQuery(associationSql);
        assertThat(Arrays.asList("3\tPaimon", "3\tPaimon")).isEqualTo(result);
    }

    private String getCreateTableSqlStr(
            String tableName, String location, boolean locationInProperties) {
        String createTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "    a INT COMMENT 'The a field',\n"
                                + "    b STRING COMMENT 'The b field'\n"
                                + ")\n"
                                + "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'",
                        tableName);

        String partforLocation;
        if (locationInProperties) {
            partforLocation =
                    String.format(
                            "TBLPROPERTIES ( "
                                    + " '"
                                    + LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY
                                    + "'='%s' );",
                            location);
        } else {
            partforLocation = String.format("location '%s'", location);
        }
        return String.join("\n", createTable, partforLocation);
    }

    private void testRWinHive(String createTableStr, String location, String tableName) {

        String insertStr = String.format("INSERT INTO %s VALUES (3, 'Paimon');", tableName);
        String selectStr = String.format("select count(*) from %s", tableName);

        hiveShell.execute(createTableStr);
        hiveShell.execute(insertStr);

        FileIO fIO = getFileIO(catalogContext, new Path(location));
        SchemaManager schemaManager = new SchemaManager(fIO, new Path(location));
        Optional<TableSchema> tableSchema = schemaManager.latest();
        assertThat(tableSchema).isPresent();

        List<String> result = hiveShell.executeQuery(selectStr);
        assertThat(result).isEqualTo(Lists.newArrayList("1"));
    }

    private void createTableWithPropertiesLocation(
            String path, RowType rowType, String hiveTableName, Options conf, boolean isExtern)
            throws Exception {
        createTable(path, rowType, hiveTableName, conf, isExtern, true);
    }

    private void createTableWithStorageLocation(
            String path, RowType rowType, String hiveTableName, Options conf, boolean isExtern)
            throws Exception {
        createTable(path, rowType, hiveTableName, conf, isExtern, false);
    }

    private void createTable(
            String path,
            RowType rowType,
            String hiveTableName,
            Options conf,
            boolean isExtern,
            boolean locationInTBProperties)
            throws Exception {
        String db = "pdb";
        FileStoreTestUtils.createFileStoreTable(
                conf,
                rowType,
                Collections.emptyList(),
                Collections.emptyList(),
                db,
                hiveTableName,
                true);
        String location = path + "/" + db + ".db" + "/" + hiveTableName;
        String extern = isExtern ? "EXTERNAL" : "";

        String s;
        if (locationInTBProperties) {
            s =
                    "CREATE "
                            + extern
                            + " TABLE  "
                            + hiveTableName
                            + " \n"
                            + "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'\n"
                            + "TBLPROPERTIES ( "
                            + " '"
                            + LocationKeyExtractor.TBPROPERTIES_LOCATION_KEY
                            + "' ='"
                            + location
                            + "' )";
        } else {
            s =
                    "CREATE "
                            + extern
                            + " TABLE "
                            + hiveTableName
                            + "\n"
                            + "STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'\n"
                            + "LOCATION '"
                            + location
                            + "';";
        }

        hiveShell.execute(s);
    }
}
