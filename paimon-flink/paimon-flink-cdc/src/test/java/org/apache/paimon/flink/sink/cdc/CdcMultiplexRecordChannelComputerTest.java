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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TraceableFileIO;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CdcRecordChannelComputer}. */
public class CdcMultiplexRecordChannelComputerTest {

    @TempDir java.nio.file.Path tempDir;
    private CatalogLoader catalogLoader;
    private Path warehouse;
    private String databaseName;
    private Identifier tableWithPartition;
    private Catalog catalog;
    private Identifier tableWithoutPartition;

    @BeforeEach
    public void before() throws Exception {
        warehouse = new Path(TraceableFileIO.SCHEME + "://" + tempDir.toString());

        databaseName = "test_db";
        tableWithPartition = Identifier.create(databaseName, "test_table1");
        tableWithoutPartition = Identifier.create(databaseName, "test_table2");

        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse.toString());
        catalogOptions.set(CatalogOptions.URI, "");
        catalogLoader = () -> CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        catalog = catalogLoader.load();
        catalog.createDatabase(databaseName, true);

        Options conf = new Options();
        conf.set(CoreOptions.BUCKET, ThreadLocalRandom.current().nextInt(1, 5));
        conf.set(CdcRecordStoreWriteOperator.RETRY_SLEEP_TIME, Duration.ofMillis(10));

        RowType rowTypeWithPartition =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE()},
                        new String[] {"pt", "k", "v"});
        RowType rowTypeWithoutPartition =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT(), DataTypes.DOUBLE(),
                        },
                        new String[] {"k", "v"});

        List<Tuple2<Identifier, Schema>> tables =
                Arrays.asList(
                        Tuple2.of(
                                tableWithPartition,
                                new Schema(
                                        rowTypeWithPartition.getFields(),
                                        Collections.singletonList("pt"),
                                        Arrays.asList("pt", "k"),
                                        conf.toMap(),
                                        "")),
                        Tuple2.of(
                                tableWithoutPartition,
                                new Schema(
                                        rowTypeWithoutPartition.getFields(),
                                        Collections.emptyList(),
                                        Collections.singletonList("k"),
                                        conf.toMap(),
                                        "")));

        for (Tuple2<Identifier, Schema> tableAndSchema : tables) {
            catalog.createTable(tableAndSchema.f0, tableAndSchema.f1, false);
        }
    }

    @AfterEach
    public void after() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    public void testSchemaWithPartition() throws Exception {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numInputs = random.nextInt(1000) + 1;
        List<Map<String, String>> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("pt", String.valueOf(random.nextInt(10) + 1));
            fields.put("k", String.valueOf(random.nextLong()));
            fields.put("v", String.valueOf(random.nextDouble()));
            input.add(fields);
        }

        testImpl(tableWithPartition, input);
    }

    @Test
    public void testSchemaNoPartition() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int numInputs = random.nextInt(1000) + 1;
        List<Map<String, String>> input = new ArrayList<>();
        for (int i = 0; i < numInputs; i++) {
            Map<String, String> fields = new HashMap<>();
            fields.put("k", String.valueOf(random.nextLong()));
            fields.put("v", String.valueOf(random.nextDouble()));
            input.add(fields);
        }

        testImpl(tableWithoutPartition, input);
    }

    private void testImpl(Identifier tableId, List<Map<String, String>> input) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numChannels = random.nextInt(10) + 1;
        CdcMultiplexRecordChannelComputer channelComputer =
                new CdcMultiplexRecordChannelComputer(catalogLoader);
        channelComputer.setup(numChannels);

        // assert that insert and delete records are routed into same channel

        for (Map<String, String> data : input) {
            CdcRecord insertRecord = new CdcRecord(RowKind.INSERT, data);
            CdcRecord deleteRecord = new CdcRecord(RowKind.DELETE, data);

            assertThat(
                            channelComputer.channel(
                                    CdcMultiplexRecord.fromCdcRecord(
                                            tableId.getDatabaseName(),
                                            tableId.getObjectName(),
                                            insertRecord)))
                    .isEqualTo(
                            channelComputer.channel(
                                    CdcMultiplexRecord.fromCdcRecord(
                                            tableId.getDatabaseName(),
                                            tableId.getObjectName(),
                                            deleteRecord)));
        }

        // assert that channel >= 0
        int numTests = random.nextInt(10) + 1;
        for (int test = 0; test < numTests; test++) {
            Map<String, String> data = input.get(random.nextInt(input.size()));
            CdcRecord record = new CdcRecord(RowKind.INSERT, data);

            int numBuckets = random.nextInt(numChannels * 4) + 1;
            for (int i = 0; i < numBuckets; i++) {
                int channel =
                        channelComputer.channel(
                                CdcMultiplexRecord.fromCdcRecord(
                                        tableId.getDatabaseName(),
                                        tableId.getObjectName(),
                                        record));
                assertThat(channel >= 0).isTrue();
            }
        }
    }
}
