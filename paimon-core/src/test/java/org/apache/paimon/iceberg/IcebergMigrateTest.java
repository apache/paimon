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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** doc. */
public class IcebergMigrateTest {
    @TempDir java.nio.file.Path iceTempDir;
    @TempDir java.nio.file.Path paiTempDir;

    String iceDatabase = "ice_db";
    String iceTable = "ice_t";

    String paiDatabase = "pai_db";
    String paiTable = "pai_t";

    Schema iceSchema =
            new Schema(
                    Types.NestedField.required(1, "k", Types.IntegerType.get()),
                    Types.NestedField.required(2, "v", Types.IntegerType.get()),
                    Types.NestedField.required(3, "dt", Types.StringType.get()),
                    Types.NestedField.required(4, "hh", Types.StringType.get()));
    Schema iceDeleteSchema =
            new Schema(
                    Types.NestedField.required(1, "k", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "v", Types.IntegerType.get()));

    PartitionSpec icePartitionSpec =
            PartitionSpec.builderFor(iceSchema).identity("dt").identity("hh").build();

    @ParameterizedTest(name = "isPartitioned = {0}")
    @ValueSource(booleans = {true, false})
    public void testMigrateOnlyAdd(boolean isPartitioned) throws Exception {
        Table icebergTable = createIcebergTable(isPartitioned);
        List<GenericRecord> records1 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "00"),
                                toIcebergRecord(2, 2, "20240101", "00"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records1, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, records1);
        }

        List<GenericRecord> records2 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "01"),
                                toIcebergRecord(2, 2, "20240101", "01"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records2, "20240101", "01");
        } else {
            writeRecordsToIceberg(icebergTable, records2);
        }

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        false,
                        1);
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(
                        paiResults.stream()
                                .map(row -> String.format("Record(%s)", row))
                                .collect(Collectors.toList()))
                .hasSameElementsAs(
                        Stream.concat(records1.stream(), records2.stream())
                                .map(GenericRecord::toString)
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest(name = "isPartitioned = {0}")
    @ValueSource(booleans = {true, false})
    public void testMigrateAddAndDelete(boolean isPartitioned) throws Exception {
        Table icebergTable = createIcebergTable(isPartitioned);
        List<GenericRecord> records1 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "00"),
                                toIcebergRecord(2, 2, "20240101", "00"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records1, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, records1);
        }

        List<GenericRecord> records2 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "01"),
                                toIcebergRecord(2, 2, "20240101", "01"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records2, "20240101", "01");
        } else {
            writeRecordsToIceberg(icebergTable, records2);
        }

        // the file written with records2 will be deleted and generate a delete manifest entry, not
        // a delete file
        icebergTable.newDelete().deleteFromRowFilter(Expressions.equal("hh", "00")).commit();

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        false,
                        1);
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(
                        paiResults.stream()
                                .map(row -> String.format("Record(%s)", row))
                                .collect(Collectors.toList()))
                .hasSameElementsAs(
                        records2.stream()
                                .map(GenericRecord::toString)
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    public void testMigrateWithDeleteFile(boolean isPartitioned, boolean ignoreDelete)
            throws Exception {
        Table icebergTable = createIcebergTable(isPartitioned);
        List<GenericRecord> records1 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "00"),
                                toIcebergRecord(2, 2, "20240101", "00"))
                        .collect(Collectors.toList());
        List<GenericRecord> deleteRecords1 =
                Collections.singletonList(toIcebergRecord(1, 1, iceDeleteSchema));

        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records1, "20240101", "00");
            writeEqualityDeleteFile(icebergTable, deleteRecords1, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, records1);
            writeEqualityDeleteFile(icebergTable, deleteRecords1);
        }

        List<GenericRecord> records2 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "01"),
                                toIcebergRecord(2, 2, "20240101", "01"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records2, "20240101", "01");
        } else {
            writeRecordsToIceberg(icebergTable, records2);
        }

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        ignoreDelete,
                        1);
        if (!ignoreDelete) {
            assertThatThrownBy(icebergMigrator::executeMigrate)
                    .rootCause()
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage(
                            "IcebergMigrator don't support analyzing manifest file with 'DELETE' content. "
                                    + "You can set 'ignore-delete' to ignore manifest file with 'DELETE' content.");
            return;
        } else {
            icebergMigrator.executeMigrate();
        }

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(
                        paiResults.stream()
                                .map(row -> String.format("Record(%s)", row))
                                .collect(Collectors.toList()))
                .hasSameElementsAs(
                        Stream.concat(records1.stream(), records2.stream())
                                .map(GenericRecord::toString)
                                .collect(Collectors.toList()));
    }

    @ParameterizedTest(name = "isPartitioned = {0}")
    @ValueSource(booleans = {true, false})
    public void testMigrateWithSchemaEvolution(boolean isPartitioned) throws Exception {
        Table icebergTable = createIcebergTable(isPartitioned);

        // write base data
        List<GenericRecord> records1 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "00"),
                                toIcebergRecord(2, 2, "20240101", "00"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records1, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, records1);
        }

        List<GenericRecord> records2 =
                Stream.of(
                                toIcebergRecord(1, 1, "20240101", "01"),
                                toIcebergRecord(2, 2, "20240101", "01"))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, records2, "20240101", "01");
        } else {
            writeRecordsToIceberg(icebergTable, records2);
        }

        //        testDeleteColumn(icebergTable, isPartitioned);
        testAddColumn(icebergTable, isPartitioned);
    }

    private void testDeleteColumn(Table icebergTable, boolean isPartitioned) throws Exception {
        icebergTable.updateSchema().deleteColumn("v").commit();
        Schema newIceSchema = icebergTable.schema();
        List<GenericRecord> addedRecords =
                Stream.of(
                                toIcebergRecord(3, "20240101", "00", newIceSchema),
                                toIcebergRecord(4, "20240101", "00", newIceSchema))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, newIceSchema, addedRecords, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, newIceSchema, addedRecords);
        }

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        false,
                        1);
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(
                        paiResults.stream()
                                .map(row -> String.format("Record(%s)", row))
                                .collect(Collectors.toList()))
                .hasSameElementsAs(
                        Stream.of(
                                        "Record(1, 20240101, 00)",
                                        "Record(2, 20240101, 00)",
                                        "Record(1, 20240101, 01)",
                                        "Record(2, 20240101, 01)",
                                        "Record(3, 20240101, 00)",
                                        "Record(4, 20240101, 00)")
                                .collect(Collectors.toList()));
    }

    private void testAddColumn(Table icebergTable, boolean isPartitioned) throws Exception {
        icebergTable.updateSchema().addColumn("v2", Types.IntegerType.get()).commit();
        Schema newIceSchema = icebergTable.schema();
        List<GenericRecord> addedRecords =
                Stream.of(
                                toIcebergRecord(3, 3, "20240101", "00", 3, newIceSchema),
                                toIcebergRecord(4, 3, "20240101", "00", 3, newIceSchema))
                        .collect(Collectors.toList());
        if (isPartitioned) {
            writeRecordsToIceberg(icebergTable, newIceSchema, addedRecords, "20240101", "00");
        } else {
            writeRecordsToIceberg(icebergTable, newIceSchema, addedRecords);
        }

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        false,
                        1);
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(
                        paiResults.stream()
                                .map(row -> String.format("Record(%s)", row))
                                .collect(Collectors.toList()))
                .hasSameElementsAs(
                        Stream.of(
                                        "Record(1, 1, 20240101, 00, null)",
                                        "Record(2, 2, 20240101, 00, null)",
                                        "Record(1, 1, 20240101, 01, null)",
                                        "Record(2, 2,  20240101, 01, null)",
                                        "Record(3, 3, 20240101, 00, 3)",
                                        "Record(4, 4, 20240101, 00, 3)")
                                .collect(Collectors.toList()));
    }

    @Test
    public void testAllDataTypes() throws Exception {
        Schema iceAllTypesSchema =
                new Schema(
                        Types.NestedField.required(1, "c1", Types.BooleanType.get()),
                        Types.NestedField.required(2, "c2", Types.IntegerType.get()),
                        Types.NestedField.required(3, "c3", Types.LongType.get()),
                        Types.NestedField.required(4, "c4", Types.FloatType.get()),
                        Types.NestedField.required(5, "c5", Types.DoubleType.get()),
                        Types.NestedField.required(6, "c6", Types.DateType.get()),
                        Types.NestedField.required(7, "c7", Types.StringType.get()),
                        Types.NestedField.required(8, "c9", Types.BinaryType.get()),
                        Types.NestedField.required(9, "c11", Types.DecimalType.of(10, 2)),
                        Types.NestedField.required(10, "c13", Types.TimestampType.withoutZone()),
                        Types.NestedField.required(11, "c14", Types.TimestampType.withZone()));
        Table icebergTable = createIcebergTable(false, iceAllTypesSchema);
        GenericRecord record = GenericRecord.create(iceAllTypesSchema);
        record.set(0, true);
        record.set(1, 1);
        record.set(2, 1L);
        record.set(3, 1.0F);
        record.set(4, 1.0D);
        record.set(5, LocalDate.of(2023, 10, 18));
        record.set(6, "test");
        record.set(7, ByteBuffer.wrap(new byte[] {1, 2, 3}));
        record.set(8, new BigDecimal("122.50"));
        record.set(9, LocalDateTime.now());
        record.set(10, OffsetDateTime.now());

        writeRecordsToIceberg(icebergTable, iceAllTypesSchema, Collections.singletonList(record));

        CatalogContext context = CatalogContext.create(new Path(paiTempDir.toString()));
        context.options().set(CACHE_ENABLED, false);
        Catalog catalog = CatalogFactory.createCatalog(context);
        IcebergMigrator icebergMigrator =
                new IcebergMigrator(
                        catalog,
                        new Path(icebergTable.location(), "metadata"),
                        paiDatabase,
                        paiTable,
                        false,
                        1);
        icebergMigrator.executeMigrate();

        FileStoreTable paimonTable =
                (FileStoreTable) catalog.getTable(Identifier.create(paiDatabase, paiTable));
        List<String> paiResults = getPaimonResult(paimonTable);
        assertThat(paiResults.size()).isEqualTo(1);
    }

    private Table createIcebergTable(boolean isPartitioned) {
        return createIcebergTable(isPartitioned, iceSchema);
    }

    private Table createIcebergTable(boolean isPartitioned, Schema icebergSchema) {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), iceTempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of(iceDatabase, iceTable);

        if (!isPartitioned) {
            return catalog.buildTable(icebergIdentifier, icebergSchema)
                    .withPartitionSpec(PartitionSpec.unpartitioned())
                    .create();
        } else {
            return catalog.buildTable(icebergIdentifier, icebergSchema)
                    .withPartitionSpec(icePartitionSpec)
                    .create();
        }
    }

    private GenericRecord toIcebergRecord(int k, String dt, String hh, Schema icebergSchema) {
        GenericRecord record = GenericRecord.create(icebergSchema);
        record.set(0, k);
        record.set(1, dt);
        record.set(2, hh);
        return record;
    }

    private GenericRecord toIcebergRecord(
            int k, int v, String dt, String hh, int v2, Schema icebergSchema) {
        GenericRecord record = GenericRecord.create(icebergSchema);
        record.set(0, k);
        record.set(1, v);
        record.set(2, dt);
        record.set(3, hh);
        record.set(4, v2);
        return record;
    }

    private GenericRecord toIcebergRecord(int k, int v, String dt, String hh) {
        GenericRecord record = GenericRecord.create(iceSchema);
        record.set(0, k);
        record.set(1, v);
        record.set(2, dt);
        record.set(3, hh);
        return record;
    }

    private GenericRecord toIcebergRecord(int k, int v, Schema icebergSchema) {
        GenericRecord record = GenericRecord.create(iceSchema);
        record.set(0, k);
        record.set(1, v);
        return record;
    }

    private DataFile writeRecordsToIceberg(
            Table icebergTable, List<GenericRecord> records, String... partitionValues)
            throws IOException {
        return writeRecordsToIceberg(icebergTable, iceSchema, records, partitionValues);
    }

    private DataFile writeRecordsToIceberg(
            Table icebergTable,
            Schema icebergSchema,
            List<GenericRecord> records,
            String... partitionValues)
            throws IOException {
        String filepath = icebergTable.location() + "/" + UUID.randomUUID();
        OutputFile file = icebergTable.io().newOutputFile(filepath);

        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(icebergSchema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

        if (partitionValues.length != 0) {
            dataWriter =
                    Parquet.writeData(file)
                            .schema(icebergSchema)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .withSpec(icePartitionSpec)
                            .withPartition(
                                    partitionKey(
                                            icePartitionSpec,
                                            icebergTable,
                                            partitionValues[0],
                                            partitionValues[1]))
                            .build();
        }

        try {
            for (GenericRecord r : records) {
                dataWriter.write(r);
            }
        } finally {
            dataWriter.close();
        }
        DataFile dataFile = dataWriter.toDataFile();
        icebergTable.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    private void writeEqualityDeleteFile(
            Table icebergTable, List<GenericRecord> deleteRecords, String... partitionValues)
            throws IOException {
        String filepath = icebergTable.location() + "/" + UUID.randomUUID();
        OutputFile file = icebergTable.io().newOutputFile(filepath);

        EqualityDeleteWriter<GenericRecord> deleteWriter =
                Parquet.writeDeletes(file)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .rowSchema(iceDeleteSchema)
                        .withSpec(PartitionSpec.unpartitioned())
                        .equalityFieldIds(1)
                        .buildEqualityWriter();
        if (partitionValues.length != 0) {
            deleteWriter =
                    Parquet.writeDeletes(file)
                            .createWriterFunc(GenericParquetWriter::buildWriter)
                            .overwrite()
                            .rowSchema(iceDeleteSchema)
                            .withSpec(icePartitionSpec)
                            .withPartition(
                                    partitionKey(
                                            icePartitionSpec,
                                            icebergTable,
                                            partitionValues[0],
                                            partitionValues[1]))
                            .equalityFieldIds(1)
                            .buildEqualityWriter();
        }

        try (EqualityDeleteWriter<GenericRecord> closableWriter = deleteWriter) {
            closableWriter.write(deleteRecords);
        }

        DeleteFile deleteFile = deleteWriter.toDeleteFile();
        icebergTable.newRowDelta().addDeletes(deleteFile).commit();
    }

    private PartitionKey partitionKey(
            PartitionSpec spec, Table icergTable, String... partitionValues) {
        Record record =
                GenericRecord.create(icergTable.schema())
                        .copy(ImmutableMap.of("dt", partitionValues[0], "hh", partitionValues[1]));

        PartitionKey partitionKey = new PartitionKey(spec, icergTable.schema());
        partitionKey.partition(record);

        return partitionKey;
    }

    private List<String> getPaimonResult(FileStoreTable paimonTable) throws Exception {
        List<Split> splits = paimonTable.newReadBuilder().newScan().plan().splits();
        TableRead read = paimonTable.newReadBuilder().newRead();
        try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
            List<String> result = new ArrayList<>();
            recordReader.forEachRemaining(
                    row ->
                            result.add(
                                    DataFormatTestUtil.toStringNoRowKind(
                                            row, paimonTable.rowType())));
            return result;
        }
    }

    private List<String> getIcebergResult(
            Function<Table, CloseableIterable<Record>> query,
            Function<Record, String> icebergRecordToString)
            throws Exception {
        HadoopCatalog icebergCatalog =
                new HadoopCatalog(new Configuration(), iceTempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of(iceDatabase, iceTable);
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(icebergIdentifier);

        CloseableIterable<Record> result = query.apply(icebergTable);
        List<String> actual = new ArrayList<>();
        for (Record record : result) {
            actual.add(icebergRecordToString.apply(record));
        }
        result.close();
        return actual;
    }
}
