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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.annotation.Minio;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.s3.MinioTestContainer;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Test for HiveCatalog location. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class HiveCatalogLocationTest {
    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    @Minio private static MinioTestContainer minioTestContainer;

    public static final String HIVE_CONF = "/hive-conf";

    private HiveCatalog catalog;

    private IMetaStoreClient hmsClient;

    private String path;

    private FileIO fileIO;

    @Before
    public void before() throws IOException {
        path = minioTestContainer.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();

        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CatalogOptions.METASTORE, "hive");
        conf.set(CatalogOptions.URI, "");
        conf.set(
                HiveCatalogOptions.HIVE_CONF_DIR,
                hiveShell.getBaseDir().getRoot().getPath() + HIVE_CONF);
        conf.set(HiveCatalogOptions.LOCATION_IN_PROPERTIES, true);

        for (Map.Entry<String, String> stringStringEntry :
                minioTestContainer.getS3ConfigOptions().entrySet()) {
            conf.set(stringStringEntry.getKey(), stringStringEntry.getValue());
        }

        // create CatalogContext using the options
        CatalogContext catalogContext = CatalogContext.create(conf);

        Path warehouse = new Path(path);
        fileIO = getFileIO(catalogContext, warehouse);
        fileIO.mkdirs(warehouse);

        HiveCatalogFactory hiveCatalogFactory = new HiveCatalogFactory();
        catalog = (HiveCatalog) hiveCatalogFactory.create(fileIO, warehouse, catalogContext);
        hmsClient = catalog.getHmsClient();
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
    }

    @Test
    public void testDBLocation() throws Exception {

        Set<String> dbs = Sets.newHashSet("db1", "db2", "db3", "db4", "db5");
        List<Path> paths = new ArrayList<>();
        for (String db : dbs) {
            catalog.createDatabase(db, true);
            Assert.assertNotNull(hmsClient.getDatabase(db));

            Path actual = catalog.databasePath(db);
            Path expected = new Path(this.path + "/" + db + ".db");
            Assert.assertTrue(fileIO.exists(expected));
            Assert.assertEquals(expected, actual);

            paths.add(expected);
        }

        HashSet<String> dbsExpected = Sets.newHashSet("db1", "db2", "db3", "db4", "db5", "default");
        Assert.assertEquals(Sets.newHashSet(catalog.listDatabases()), dbsExpected);

        for (String db : dbs) {
            catalog.dropDatabase(db, false, true);
        }

        for (Path p : paths) {
            Assert.assertFalse(fileIO.exists(p));
        }

        Assert.assertEquals(Sets.newHashSet(catalog.listDatabases()), Sets.newHashSet("default"));
    }

    @Test
    public void testTableLocation() throws Exception {

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
        String expected = this.path + "/" + db + ".db" + "/" + table;
        Assert.assertTrue(fileIO.exists(new Path(expected)));
        Assert.assertEquals(expected, location);
    }
}
