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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.object.ObjectTable;
import org.apache.paimon.types.DataTypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.TrashPolicy;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.List;

import static org.apache.paimon.CoreOptions.TYPE;
import static org.apache.paimon.TableType.OBJECT_TABLE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PaimonVirtualFileSystem}. */
public abstract class VirtualFileSystemTest {

    @TempDir java.nio.file.Path tempFile;
    protected String warehouse;
    protected FileIO fileIO;
    protected RESTCatalog catalog;
    protected ConfigResponse config;
    protected Options options = new Options();
    protected FileSystem vfs;
    protected Path vfsRoot;

    @BeforeEach
    public void setUp() throws Exception {
        warehouse = tempFile.toUri().toString();
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            List<String> dbs = catalog.listDatabases();
            for (String db : dbs) {
                try {
                    catalog.dropDatabase(db, true, true);
                } catch (Exception ignored) {
                }
            }
            catalog.close();
        }
    }

    protected void createObjectTable(String databaseName, String tableName) throws Exception {
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, tableName);
        Schema schema = Schema.newBuilder().option(TYPE.key(), OBJECT_TABLE.toString()).build();
        catalog.createTable(identifier, schema, false);
        Table table = catalog.getTable(identifier);
        assertThat(table).isInstanceOf(ObjectTable.class);
    }

    protected void createNormalTable(String databaseName, String tableName) throws Exception {
        catalog.createDatabase(databaseName, true);
        Identifier identifier = Identifier.create(databaseName, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build();
        catalog.createTable(identifier, schema, false);
        Table table = catalog.getTable(identifier);
        assertThat(table).isInstanceOf(FileStoreTable.class);
    }

    protected void createDatabase(String databaseName) throws Exception {
        catalog.createDatabase(databaseName, true);
    }

    protected void checkTableExist(String databaseName, String tableName, boolean expect)
            throws Exception {
        try {
            Table table = catalog.getTable(Identifier.create(databaseName, tableName));
            Assert.assertEquals(expect, true);
        } catch (Catalog.TableNotExistException e) {
            Assert.assertEquals(expect, false);
        }
    }

    @Test
    public void testMkdir() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir");
        vfs.mkdirs(vfsPath);
        FileStatus fileStatus = vfs.getFileStatus(vfsPath);
        Assert.assertEquals(vfsPath.toString(), fileStatus.getPath().toString());
        Assert.assertTrue(fileStatus.isDirectory());
        Assert.assertEquals(0, fileStatus.getBlockSize());

        // Mkdir in non-existing table
        tableName = "object_table2";
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir");
        Assert.assertTrue(vfs.mkdirs(vfsPath));
        Table table = catalog.getTable(new Identifier(databaseName, tableName));
        assertThat(table).isInstanceOf(ObjectTable.class);
        fileStatus = vfs.getFileStatus(vfsPath);
        Assert.assertEquals(vfsPath.toString(), fileStatus.getPath().toString());
    }

    @Test
    public void testVirtualMkdir() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";

        // Mkdir for root is not supported
        try {
            vfs.mkdirs(vfsRoot);
            Assert.fail();
        } catch (IOException e) {
        }

        // Create database in virtual file system
        Path vfsPath = new Path(vfsRoot, databaseName);
        Assert.assertTrue(vfs.mkdirs(vfsPath));
        Database database = catalog.getDatabase(databaseName);
        Assert.assertEquals(databaseName, database.name());

        // Create object table in virtual file system
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName);
        Assert.assertTrue(vfs.mkdirs(vfsPath));
        Table table = catalog.getTable(new Identifier(databaseName, tableName));
        assertThat(table).isInstanceOf(ObjectTable.class);

        // Create object table with database recursively created
        databaseName = "test_db2";
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName);
        Assert.assertTrue(vfs.mkdirs(vfsPath));
        table = catalog.getTable(new Identifier(databaseName, tableName));
        assertThat(table).isInstanceOf(ObjectTable.class);
    }

    @Test
    public void testCreate() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        FSDataOutputStream out = vfs.create(vfsPath);
        out.write("hello".getBytes());
        out.close();

        FileStatus fileStatus = vfs.getFileStatus(vfsPath);
        Assert.assertEquals(vfsPath.toString(), fileStatus.getPath().toString());
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(5, fileStatus.getLen());
        Assert.assertEquals(128 * 1024 * 1024L, fileStatus.getBlockSize());

        FSDataInputStream in = vfs.open(vfsPath);
        byte[] buffer = new byte[5];
        in.read(buffer);
        in.close();
        Assert.assertArrayEquals("hello".getBytes(), buffer);

        // Create file in non-existing table
        tableName = "object_table2";
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        out = vfs.create(vfsPath);
        out.write("hello".getBytes());
        out.close();
        in = vfs.open(vfsPath);
        buffer = new byte[5];
        in.read(buffer);
        in.close();
        Assert.assertArrayEquals("hello".getBytes(), buffer);
    }

    @Test
    public void testVirtualCreate() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        // Create root
        try {
            vfs.create(vfsRoot);
            Assert.fail();
        } catch (IOException e) {
        }

        // Create file for database
        try {
            vfs.create(new Path(vfsRoot, databaseName));
            Assert.fail();
        } catch (IOException e) {
        }

        // Create file for table
        try {
            vfs.create(new Path(vfsRoot, databaseName + "/" + tableName));
            Assert.fail();
        } catch (IOException e) {
        }
    }

    @Test
    public void testListStatus() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath1 = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir");
        vfs.mkdirs(vfsPath1);
        Path vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName + "/file.txt");
        FSDataOutputStream out = vfs.create(vfsPath2);
        out.write("hello".getBytes());
        out.close();

        FileStatus[] fileStatuses =
                vfs.listStatus(new Path(vfsRoot, databaseName + "/" + tableName));
        Assert.assertEquals(2, fileStatuses.length);
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.getPath().toString().equals(vfsPath1.toString())) {
                Assert.assertTrue(fileStatus.isDirectory());
            } else if (fileStatus.getPath().toString().equals(vfsPath2.toString())) {
                Assert.assertTrue(fileStatus.isFile());
                Assert.assertEquals(5, fileStatus.getLen());
            }
        }

        // List in non-existing table
        tableName = "object_table2";
        try {
            vfs.listStatus(new Path(vfsRoot, databaseName + "/" + tableName));
            Assert.fail();
        } catch (IOException e) {
        }
    }

    @Test
    public void testVirtualListStatus() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        // List root
        FileStatus[] fileStatuses = vfs.listStatus(vfsRoot);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertEquals(
                new Path(vfsRoot, databaseName).toString(), fileStatuses[0].getPath().toString());
        Assert.assertTrue(fileStatuses[0].isDirectory());

        // List database
        fileStatuses = vfs.listStatus(new Path(vfsRoot, databaseName));
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertEquals(
                new Path(vfsRoot, databaseName + "/" + tableName).toString(),
                fileStatuses[0].getPath().toString());
        Assert.assertTrue(fileStatuses[0].isDirectory());
    }

    @Test
    public void testRename() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        FSDataOutputStream out = vfs.create(vfsPath);
        out.write("hello".getBytes());
        out.close();

        Path vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file2.txt");
        Assert.assertTrue(vfs.rename(vfsPath, vfsPath2));

        Assert.assertFalse(vfs.exists(vfsPath));
        FileStatus fileStatus = vfs.getFileStatus(vfsPath2);
        Assert.assertEquals(vfsPath2.toString(), fileStatus.getPath().toString());
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(5, fileStatus.getLen());

        // Rename to table root: /database/table/test_dir/file2.txt -> /database/table/
        // which actually means: /database/table/test_dir/file2.txt -> /database/table/file2.txt
        Path vfsPath3 = new Path(vfsRoot, databaseName + "/" + tableName);
        Assert.assertTrue(vfs.rename(vfsPath2, vfsPath3));
        fileStatus = vfs.getFileStatus(new Path(vfsPath3, "file2.txt"));
        Assert.assertEquals(
                new Path(vfsPath3, "file2.txt").toString(), fileStatus.getPath().toString());
        Assert.assertTrue(fileStatus.isFile());
        Assert.assertEquals(5, fileStatus.getLen());

        // Rename in non-existing table
        String tableName2 = "object_table2";
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName2 + "/test_dir/file.txt");
        vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName2 + "/test_dir/file2.txt");
        try {
            vfs.rename(vfsPath, vfsPath2);
            Assert.fail();
        } catch (IOException e) {
        }

        // Rename cross table is not supported
        createObjectTable(databaseName, tableName2);
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file2.txt");
        vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName2 + "/test_dir/file2.txt");
        try {
            vfs.rename(vfsPath, vfsPath2);
            Assert.fail();
        } catch (IOException e) {
        }
    }

    @Test
    public void testVirtualRename() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createNormalTable(databaseName, tableName);
        Assert.assertTrue(vfs.exists(new Path(vfsRoot, databaseName + "/" + tableName)));
        // Rename root
        Path vfsPath = vfsRoot;
        Path vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file2.txt");
        Assert.assertFalse(vfs.rename(vfsPath, vfsPath2));
        Assert.assertTrue(vfs.exists(new Path(vfsRoot, databaseName + "/" + tableName)));

        // Rename database is not supported
        vfsPath = new Path(vfsRoot, databaseName);
        vfsPath2 = new Path(vfsRoot, databaseName + "_2");
        try {
            vfs.rename(vfsPath, vfsPath2);
            Assert.fail();
        } catch (IOException e) {
        }
        Assert.assertTrue(vfs.exists(new Path(vfsRoot, databaseName + "/" + tableName)));

        // Rename table
        // 1. table -> table_2
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName);
        vfsPath2 = new Path(vfsRoot, databaseName + "/" + tableName + "_2");
        Assert.assertTrue(vfs.rename(vfsPath, vfsPath2));
        checkTableExist(databaseName, tableName, false);
        checkTableExist(databaseName, tableName + "_2", true);
        // 2. table not exists
        Assert.assertFalse(vfs.rename(vfsPath, vfsPath2));
        Assert.assertFalse(vfs.rename(vfsPath, vfsPath));
        // 3. src = dst
        Assert.assertTrue(vfs.rename(vfsPath2, vfsPath2));
        // 4. rename across database
        try {
            vfs.rename(vfsPath2, new Path(vfsRoot, databaseName + "_2/" + tableName));
            Assert.fail();
        } catch (IOException e) {
        }
    }

    @Test
    public void testDelete() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        FSDataOutputStream out = vfs.create(vfsPath);
        out.write("hello".getBytes());
        out.close();
        Assert.assertTrue(vfs.delete(vfsPath, false));
        Assert.assertFalse(vfs.exists(vfsPath));

        // Delete in non-existing table
        tableName = "object_table2";
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        try {
            vfs.delete(vfsPath, false);
            Assert.fail();
        } catch (IOException e) {
        }
    }

    @Test
    public void testVirtualDelete() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);
        // Delete root is not supported
        Path vfsPath = vfsRoot;
        try {
            vfs.delete(vfsPath, false);
            Assert.fail();
        } catch (IOException e) {
        }

        // Delete database
        // 1. recursive = false
        vfsPath = new Path(vfsRoot, databaseName);
        try {
            vfs.delete(vfsPath, false);
            Assert.fail();
        } catch (IOException e) {
        }
        // 2. recursive = true
        Assert.assertTrue(vfs.delete(vfsPath, true));
        Assert.assertFalse(vfs.exists(vfsPath));
        // 3. non-exist database, return false
        Assert.assertFalse(vfs.delete(new Path(vfsRoot, databaseName + "_2"), true));

        // Delete table
        // 1. existing table
        createObjectTable(databaseName, tableName);
        vfsPath = new Path(vfsRoot, databaseName + "/" + tableName);
        Assert.assertTrue(vfs.delete(vfsPath, false));
        Assert.assertFalse(vfs.exists(vfsPath));

        // 2. non-exist table, return false
        Assert.assertFalse(
                vfs.delete(new Path(vfsRoot, databaseName + "/" + tableName + "_2"), true));
        Assert.assertFalse(
                vfs.delete(new Path(vfsRoot, databaseName + "_2/" + tableName + "_2"), true));
    }

    @Test
    public void testVisitNormalTable() throws Exception {
        String databaseName = "test_db";
        String tableName = "normal_table";
        createNormalTable(databaseName, tableName);
        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName);
        // List normal table, directory schema should exist
        FileStatus fileStatus = vfs.getFileStatus(vfsPath);
        Assert.assertTrue(fileStatus.isDirectory());
        Assert.assertEquals(vfsPath.toString(), fileStatus.getPath().toString());
        FileStatus[] fileStatuses = vfs.listStatus(vfsPath);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].isDirectory());
        Assert.assertEquals(
                new Path(vfsPath, "schema").toString(), fileStatuses[0].getPath().toString());
    }

    @Test
    public void testTrash() throws Exception {
        String databaseName = "test_db";
        String tableName = "object_table";
        createObjectTable(databaseName, tableName);

        Path vfsPath = new Path(vfsRoot, databaseName + "/" + tableName + "/test_dir/file.txt");
        FSDataOutputStream out = vfs.create(vfsPath);
        out.write("hello".getBytes());
        out.close();

        // Trash vfsPath, return false and trash action not executed
        TrashPolicy trashPolicy = TrashPolicy.getInstance(new Configuration(), vfs);
        Assert.assertFalse(trashPolicy.moveToTrash(vfsPath));
        Assert.assertTrue(vfs.exists(vfsPath));
    }
}
