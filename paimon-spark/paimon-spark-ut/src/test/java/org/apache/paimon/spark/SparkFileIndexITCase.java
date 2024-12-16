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

package org.apache.paimon.spark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RoaringBitmap32;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for using file index in Spark. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkFileIndexITCase extends SparkWriteITCase {

    protected FileIO fileIO = LocalFileIO.create();
    protected FileSystemCatalog fileSystemCatalog;

    @BeforeAll
    public void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:///" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .master("local[1]")
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        spark.conf().set("spark.sql.shuffle.partitions", 1);
        spark.conf().set("spark.default.parallelism", 1);
        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");

        Options options = new Options();
        options.set(WAREHOUSE, spark.conf().get("spark.sql.catalog.paimon.warehouse"));
        options.set(CACHE_ENABLED, false);
        fileSystemCatalog =
                (FileSystemCatalog) CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    @Test
    public void testReadWriteTableWithBitmapIndex() throws Catalog.TableNotExistException {

        spark.sql(
                "CREATE TABLE T(a int) TBLPROPERTIES ("
                        + "'file-index.bitmap.columns'='a',"
                        + "'file-index.in-manifest-threshold'='1B');");
        spark.sql("INSERT INTO T VALUES (0),(1),(2),(3),(4),(5);");

        List<Row> rows1 = spark.sql("SELECT a FROM T where a>3;").collectAsList();
        assertThat(rows1.toString()).isEqualTo("[[4], [5]]");

        // check query result
        List<Row> rows2 = spark.sql("SELECT a FROM T where a=3;").collectAsList();
        assertThat(rows2.toString()).isEqualTo("[[3]]");

        // check index reader
        foreachIndexReader(
                fileIndexReader -> {
                    FileIndexResult fileIndexResult =
                            fileIndexReader.visitEqual(new FieldRef(0, "", new IntType()), 3);
                    assert fileIndexResult instanceof BitmapIndexResult;
                    RoaringBitmap32 roaringBitmap32 = ((BitmapIndexResult) fileIndexResult).get();
                    assert roaringBitmap32.equals(RoaringBitmap32.bitmapOf(3));
                });
    }

    @Test
    public void testReadWriteTableWithBitSliceIndex() throws Catalog.TableNotExistException {

        spark.sql(
                "CREATE TABLE T(a int) TBLPROPERTIES ("
                        + "'file-index.bsi.columns'='a',"
                        + "'file-index.in-manifest-threshold'='1B');");
        spark.sql("INSERT INTO T VALUES (0),(1),(2),(3),(4),(5);");

        // check query result
        List<Row> rows = spark.sql("SELECT a FROM T where a>=3;").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[3], [4], [5]]");

        // check index reader
        foreachIndexReader(
                fileIndexReader -> {
                    FileIndexResult fileIndexResult =
                            fileIndexReader.visitGreaterOrEqual(
                                    new FieldRef(0, "", new IntType()), 3);
                    assertThat(fileIndexResult).isInstanceOf(BitmapIndexResult.class);
                    RoaringBitmap32 roaringBitmap32 = ((BitmapIndexResult) fileIndexResult).get();
                    assertThat(roaringBitmap32).isEqualTo(RoaringBitmap32.bitmapOf(3, 4, 5));
                });
    }

    protected void foreachIndexReader(Consumer<FileIndexReader> consumer)
            throws Catalog.TableNotExistException {
        Path tableRoot = fileSystemCatalog.getTableLocation(Identifier.create("db", "T"));
        SchemaManager schemaManager = new SchemaManager(fileIO, tableRoot);
        FileStorePathFactory pathFactory =
                new FileStorePathFactory(
                        tableRoot,
                        RowType.of(),
                        new CoreOptions(new Options()).partitionDefaultName(),
                        CoreOptions.FILE_FORMAT.defaultValue(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.PARTITION_GENERATE_LEGCY_NAME.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);

        Table table = fileSystemCatalog.getTable(Identifier.create("db", "T"));
        ReadBuilder readBuilder = table.newReadBuilder();
        List<Split> splits = readBuilder.newScan().plan().splits();
        for (Split split : splits) {
            DataSplit dataSplit = (DataSplit) split;
            DataFilePathFactory dataFilePathFactory =
                    pathFactory.createDataFilePathFactory(
                            dataSplit.partition(), dataSplit.bucket());
            for (DataFileMeta dataFileMeta : dataSplit.dataFiles()) {
                TableSchema tableSchema = schemaManager.schema(dataFileMeta.schemaId());
                List<String> indexFiles =
                        dataFileMeta.extraFiles().stream()
                                .filter(
                                        name ->
                                                name.endsWith(
                                                        DataFilePathFactory.INDEX_PATH_SUFFIX))
                                .collect(Collectors.toList());
                // assert index file exist and only one index file
                assert indexFiles.size() == 1;
                try (FileIndexFormat.Reader reader =
                        FileIndexFormat.createReader(
                                fileIO.newInputStream(
                                        dataFilePathFactory.toPath(indexFiles.get(0))),
                                tableSchema.logicalRowType())) {
                    Optional<FileIndexReader> fileIndexReader =
                            reader.readColumnIndex("a").stream().findFirst();
                    // assert index reader exist
                    assert fileIndexReader.isPresent();
                    consumer.accept(fileIndexReader.get());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
