package org.apache.paimon.iceberg;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;

/** doc. */
public class IcebergMigrateTest {
    @TempDir java.nio.file.Path tempDir;
    @TempDir java.nio.file.Path tempDirPaimon;

    @Test
    public void test1() throws Exception {
        Table icebergTable = createIcebergTable("default", "test");
        IcebergPathFactory icebergPathFactory =
                new IcebergPathFactory(new Path(icebergTable.location(), "metadata"));

        CatalogContext context = CatalogContext.create(new Path(tempDirPaimon.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(catalog, icebergPathFactory, "default", "tb1");
        icebergMigrator.executeMigrate();
    }

    @Test
    public void test2() throws Exception {
        IcebergPathFactory icebergPathFactory =
                new IcebergPathFactory(new Path("/Users/catyeah/Desktop", "metadata1"));

        CatalogContext context = CatalogContext.create(new Path(tempDirPaimon.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(catalog, icebergPathFactory, "default", "tb1");
        icebergMigrator.executeMigrate();
    }

    @Test
    public void test3() throws Exception {
        List<String> iceResults =
                getIcebergResult(
                        icebergTable -> IcebergGenerics.read(icebergTable).build(),
                        Record::toString);
        IcebergPathFactory icebergPathFactory =
                new IcebergPathFactory(
                        new Path("/Users/catyeah/testHome/icebergtest/mydb.db/t", "metadata"));

        CatalogContext context = CatalogContext.create(new Path(tempDirPaimon.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(catalog, icebergPathFactory, "default", "tb1");
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create("default", "tb1"));
        List<String> paiResults = getPaimonResult(paimonTable);
        System.out.println();
    }

    private Table createIcebergTable(String database, String tableName) throws Exception {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of(database, tableName);

        Schema schema =
                new Schema(
                        Types.NestedField.required(1, "id", Types.IntegerType.get()),
                        Types.NestedField.required(2, "name", Types.StringType.get()));

        PartitionSpec spec = PartitionSpec.unpartitioned();

        return catalog.buildTable(icebergIdentifier, schema).withPartitionSpec(spec).create();
    }

    private List<String> getPaimonResult(FileStoreTable paimonTable) throws Exception {
        List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
        TableRead read = paimonTable.newReadBuilder().newRead();
        try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row ->
                            result.add(
                                    DataFormatTestUtil.internalRowToString(
                                            row, paimonTable.rowType())));
            return result;
        }
    }

    private List<String> getIcebergResult(
            Function<Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog =
                new HadoopCatalog(new Configuration(), "/Users/catyeah/testHome/icebergtest");
        TableIdentifier icebergIdentifier = TableIdentifier.of("mydb.db", "t");
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);
        List<org.apache.iceberg.ManifestFile> manifestFiles =
                icebergTable.currentSnapshot().allManifests(icebergTable.io());

        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }

    //    private void writeData(Table table) {
    //        File dataDir = new File("/tmp/iceberg_data"); // 本地或远程文件系统路径
    //        dataDir.mkdirs(); // 创建目录
    //        OutputFile outputFile = Files.localOutput("/tmp/iceberg_data/table.parquet");
    //
    //        FileAppender<Record> appender =
    //                GenericParquetWriter.buildWriter(table.schema(), table.spec(), outputFile,
    // null);
    //
    //        try (appender) {
    //            // 创建示例数据记录
    //            GenericRecord record = GenericRecord.create(table.schema());
    //            appender.add(record.copy("id", 1, "name", "Alice"));
    //            appender.add(record.copy("id", 2, "name", "Bob"));
    //        }
    //
    //        // 将数据文件追加为一个新的数据文件
    //        DataFile dataFile =
    //                DataFiles.builder(table.spec())
    //                        .withInputFile(new Files.LocalInputFile(new File(dataDir,
    // "table.parquet")))
    //                        .withRecordCount(2)
    //                        .build();
    //
    //        // 提交事务
    //        table.newAppend().appendFile(dataFile).commit();
    //    }
}
