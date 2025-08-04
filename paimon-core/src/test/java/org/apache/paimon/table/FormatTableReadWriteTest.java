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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test write read equals in primary key table. */
public class FormatTableReadWriteTest extends TableTestBase {

    private Table createTable(String format) throws Exception {
        catalog.createTable(identifier(format), schema(format), true);
        return catalog.getTable(identifier(format));
    }

    private Schema schema(String format) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.INT());
        schemaBuilder.column("f2", DataTypes.SMALLINT());
        schemaBuilder.option("file.format", format);
        schemaBuilder.option("type", "format-table");
        return schemaBuilder.build();
    }

    @Test
    public void testAllFormatReadWrite() throws Exception {
        testReadWrite("orc");
        testReadWrite("parquet");
    }

    @Override
    protected void write(Table table, IOManager ioManager, InternalRow... rows) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite()) {
            write.withIOManager(ioManager);
            for (InternalRow row : rows) {
                write.write(row);
            }
            write.prepareCommit();
        }
    }

    private void testReadWrite(String format) throws Exception {
        Table table = createTable(format);

        InternalRow[] datas = datas(200);

        write(table, datas);

        List<InternalRow> readed = read(table);

        assertThat(readed).containsExactlyInAnyOrder(datas);
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
        return GenericRow.of(RANDOM.nextInt(), RANDOM.nextInt(), (short) RANDOM.nextInt());
    }
}
