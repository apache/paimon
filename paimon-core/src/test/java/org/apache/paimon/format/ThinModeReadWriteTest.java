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

package org.apache.paimon.format;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;

/** This class test the compatibility and effectiveness of storage thin mode. */
public class ThinModeReadWriteTest extends TableTestBase {

    private Table createTable(String format, Boolean thinMode) throws Exception {
        catalog.createTable(identifier(), schema(format, thinMode), true);
        return catalog.getTable(identifier());
    }

    private Schema schema(String format, Boolean thinMode) {
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
        schemaBuilder.primaryKey(
                "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
                "f13");
        schemaBuilder.option("bucket", "1");
        schemaBuilder.option("bucket-key", "f1");
        schemaBuilder.option("file.format", format);
        schemaBuilder.option("storage.thin-mode", thinMode.toString());
        return schemaBuilder.build();
    }

    @Test
    public void testThinModeWorks() throws Exception {

        InternalRow[] datas = datas(200000);

        Table table = createTable("orc", true);
        write(table, datas);

        long size1 = tableSize(table);
        dropTableDefault();

        table = createTable("orc", false);
        write(table, datas);
        long size2 = tableSize(table);
        dropTableDefault();

        Assertions.assertThat(size2).isGreaterThan(size1);
    }

    @Test
    public void testAllFormatReadWrite() throws Exception {
        testFormat("orc");
        testFormat("parquet");
        testFormat("avro");
    }

    private void testFormat(String format) throws Exception {
        testReadWrite(format, true, true);
        testReadWrite(format, true, false);
        testReadWrite(format, false, true);
        testReadWrite(format, false, false);
    }

    private void testReadWrite(String format, boolean writeThin, boolean readThin)
            throws Exception {
        Table tableWrite = createTable(format, writeThin);
        Table tableRead = setThinMode(tableWrite, readThin);

        InternalRow[] datas = datas(2000);

        write(tableWrite, datas);

        List<InternalRow> readed = read(tableRead);

        Assertions.assertThat(readed).containsExactlyInAnyOrder(datas);
        dropTableDefault();
    }

    private Table setThinMode(Table table, Boolean flag) {
        return table.copy(
                new HashMap() {
                    {
                        put("storage.thin-mode", flag.toString());
                    }
                });
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

    public static long tableSize(Table table) throws Exception {
        long count = 0;
        List<ManifestEntry> files =
                ((FileStoreTable) table).store().newScan().plan().files(FileKind.ADD);
        for (ManifestEntry file : files) {
            count += file.file().fileSize();
        }

        return count;
    }
}
