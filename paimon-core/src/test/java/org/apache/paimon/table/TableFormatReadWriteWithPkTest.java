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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Test write read equals in primary key table. */
public class TableFormatReadWriteWithPkTest extends TableTestBase {

    private Table createTable(String format) throws Exception {
        catalog.createTable(identifier(format), schema(format), true);
        return catalog.getTable(identifier(format));
    }

    private Schema schema(String format) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.column("f3", DataTypes.STRING());
        schemaBuilder.column("f4", DataTypes.DOUBLE());
        schemaBuilder.column("f5", DataTypes.CHAR(100));
        schemaBuilder.column("f6", DataTypes.VARCHAR(100));
        schemaBuilder.column("f7", DataTypes.BOOLEAN());
        schemaBuilder.column("f8", DataTypes.INT());
        schemaBuilder.column("f9", DataTypes.TIME());
        schemaBuilder.column("f10", DataTypes.TIMESTAMP());
        schemaBuilder.column("f11", DataTypes.DECIMAL(10, 2));
        schemaBuilder.column("f12", DataTypes.BYTES());
        schemaBuilder.column("f13", DataTypes.FLOAT());
        schemaBuilder.column("f14", DataTypes.BINARY(100));
        schemaBuilder.column("f15", DataTypes.VARBINARY(100));
        schemaBuilder.primaryKey("f0", "f1", "f3");
        schemaBuilder.partitionKeys("f0");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("bucket-key", "f1");
        schemaBuilder.option("file.format", format);
        return schemaBuilder.build();
    }

    @Test
    public void testAllFormatReadWrite() throws Exception {
        testReadWrite("orc");
        testReadWrite("parquet");
        testReadWrite("avro");
    }

    private void testReadWrite(String format) throws Exception {
        Table table = createTable(format);

        InternalRow[] datas = datas(200);

        write(table, datas);

        List<InternalRow> readed = read(table);

        Assertions.assertThat(readed).containsExactlyInAnyOrder(datas);
        dropTableDefault();
    }

    public void dropTableDefault() throws Exception {
        catalog.dropTable(identifier(), true);
    }

    InternalRow[] datas(int i) {
        InternalRow[] arrays = new InternalRow[i];
        for (int j = 0; j < i; j++) {
            arrays[j] = data();
        }
        return arrays;
    }

    protected InternalRow data() {
        return GenericRow.of(
                RANDOM.nextInt(),
                RANDOM.nextInt(),
                (short) RANDOM.nextInt(),
                randomString(),
                RANDOM.nextDouble(),
                randomString(),
                randomString(),
                RANDOM.nextBoolean(),
                RANDOM.nextInt(),
                RANDOM.nextInt(),
                Timestamp.now(),
                Decimal.zero(10, 2),
                randomBytes(),
                (float) RANDOM.nextDouble(),
                randomBytes(),
                randomBytes());
    }
}
