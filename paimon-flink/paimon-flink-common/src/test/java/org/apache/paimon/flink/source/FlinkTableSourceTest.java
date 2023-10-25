/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.paimon.flink.source;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Test for {@link FlinkTableSource}. */
public class FlinkTableSourceTest extends TableTestBase {

    @Test
    public void testListPartition() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, "T"));
        Schema schema =
                Schema.newBuilder()
                        .column("col1", DataTypes.INT())
                        .column("col2", DataTypes.TINYINT())
                        .column("col3", DataTypes.SMALLINT())
                        .column("col4", DataTypes.BIGINT())
                        .column("col5", DataTypes.STRING())
                        .column("col6", DataTypes.DOUBLE())
                        .column("col7", DataTypes.CHAR(1))
                        .column("col8", DataTypes.VARCHAR(1))
                        .column("col9", DataTypes.BOOLEAN())
                        .column("col10", DataTypes.FLOAT())
                        .column("col11", DataTypes.DECIMAL(10, 4))
                        .column("colN", DataTypes.VARCHAR(1))
                        .partitionKeys(
                                "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8",
                                "col9", "col11")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        Table table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);

        FlinkTableSource tableSource =
                new DataTableSource(
                        ObjectIdentifier.of("catalog1", "db1", "T"), table, false, null, null);

        write(
                table,
                GenericRow.of(
                        1, // int
                        (byte) 1, // tinyint
                        (short) 1, // smallint
                        1L, // bigint
                        BinaryString.fromString("haha"), // string
                        1.001, // double
                        BinaryString.fromString("a"), // char
                        BinaryString.fromString("a"), // varchar
                        false, // boolean
                        1.0f, // float
                        Decimal.fromBigDecimal(new BigDecimal("1000.0001"), 10, 4), // decimal
                        BinaryString.fromString("data")));

        write(
                table,
                GenericRow.of(
                        2, // int
                        (byte) 1, // tinyint
                        (short) 1, // smallint
                        1L, // bigint
                        BinaryString.fromString("haha"), // string
                        1.001, // double
                        BinaryString.fromString("a"), // char
                        BinaryString.fromString("a"), // varchar
                        false, // boolean
                        1.0f, // float
                        Decimal.fromBigDecimal(new BigDecimal("1000.0001"), 10, 4), // decimal
                        BinaryString.fromString("data")));

        Optional<List<Map<String, String>>> partitions = tableSource.listPartitions();
        Assertions.assertThat(partitions).isPresent();
        Assertions.assertThat(partitions.get()).hasSize(2);
        List<Map<String, String>> sorted =
                partitions.get().stream().map(TreeMap::new).collect(Collectors.toList());
        Assertions.assertThat(sorted.toString())
                .isEqualTo(
                        "[{col1=1, col11=1000.0001, col2=1, col3=1, col4=1, col5=haha, col6=1.001, col7=a, col8=a, col9=false}, "
                                + "{col1=2, col11=1000.0001, col2=1, col3=1, col4=1, col5=haha, col6=1.001, col7=a, col8=a, col9=false}]");
    }
}
