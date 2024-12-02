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
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.options.CatalogOptions.CACHE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

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

    PartitionSpec icePartitionSpec =
            PartitionSpec.builderFor(iceSchema).identity("dt").identity("hh").build();

    @ParameterizedTest(name = "isPartitioned = {0}")
    @ValueSource(booleans = {true, false})
    public void testMigrateOnlyAddedData(boolean isPartitioned) throws Exception {
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

    private Table createIcebergTable(boolean isPartitioned) {
        HadoopCatalog catalog = new HadoopCatalog(new Configuration(), iceTempDir.toString());
        TableIdentifier icebergIdentifier = TableIdentifier.of(iceDatabase, iceTable);

        if (!isPartitioned) {
            return catalog.buildTable(icebergIdentifier, iceSchema)
                    .withPartitionSpec(PartitionSpec.unpartitioned())
                    .create();
        } else {
            return catalog.buildTable(icebergIdentifier, iceSchema)
                    .withPartitionSpec(icePartitionSpec)
                    .create();
        }
    }

    private GenericRecord toIcebergRecord(int k, int v, String dt, String hh) {
        GenericRecord record = GenericRecord.create(iceSchema);
        record.set(0, k);
        record.set(1, v);
        record.set(2, dt);
        record.set(3, hh);
        return record;
    }

    private DataFile writeRecordsToIceberg(
            Table icebergTable, List<GenericRecord> records, String... partitionValues)
            throws IOException {
        String filepath = icebergTable.location() + "/" + UUID.randomUUID();
        OutputFile file = icebergTable.io().newOutputFile(filepath);

        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(iceSchema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

        if (partitionValues.length != 0) {
            dataWriter =
                    Parquet.writeData(file)
                            .schema(iceSchema)
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
}
