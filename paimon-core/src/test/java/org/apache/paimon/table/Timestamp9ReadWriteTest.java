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

package org.apache.paimon.table;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for timestamp 9 read write. */
public class Timestamp9ReadWriteTest extends TableTestBase {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

    @Test
    public void testTimestamp9() throws Exception {
        createTableDefault();

        LocalDateTime localDateTime =
                LocalDateTime.parse("1899-01-01 00:59:20.001001001", TIMESTAMP_FORMATTER);
        GenericRow record2 =
                GenericRow.of(
                        20,
                        1,
                        -1234,
                        Timestamp.fromLocalDateTime(localDateTime),
                        Decimal.fromBigDecimal(new BigDecimal("-123456789987654321.45678"), 23, 5),
                        "bad".getBytes(StandardCharsets.UTF_8));

        Table table = getTableDefault();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.write(record2);
            writeBuilder.newCommit().commit(write.prepareCommit());
        }

        table.newReadBuilder()
                .newRead()
                .createReader(table.newReadBuilder().newScan().plan())
                .forEachRemaining(
                        row ->
                                assertThat(row.getTimestamp(3, 9))
                                        .isEqualTo(Timestamp.fromLocalDateTime(localDateTime)));
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.partitionKeys("f1");
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.INT());
        schemaBuilder.column("f3", DataTypes.DATE());
        schemaBuilder.column("f4", DataTypes.TIMESTAMP(9));
        schemaBuilder.column("f5", DataTypes.DECIMAL(23, 5));
        schemaBuilder.column("f6", DataTypes.BYTES());
        return schemaBuilder.build();
    }
}
