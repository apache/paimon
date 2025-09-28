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

package org.apache.paimon.flink.source;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.RESTCatalogITCaseBase;
import org.apache.paimon.format.FileFormatFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.format.parquet.ParquetFileFormatFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTToken;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for format table. */
public class PaimonFormatTableTest extends RESTCatalogITCaseBase {

    @Test
    public void testParquetFileFormat() throws IOException {
        FileFormatFactory formatFactory = new ParquetFileFormatFactory();
        RowType rowType =
                RowType.builder()
                        .field("a", DataTypes.INT())
                        .field("b", DataTypes.INT())
                        .field("c", DataTypes.INT())
                        .build();
        FormatWriterFactory factory =
                (formatFactory.create(
                                new FileFormatFactory.FormatContext(new Options(), 1024, 1024)))
                        .createWriterFactory(rowType);
        InternalRow[] datas = new InternalRow[2];
        datas[0] = GenericRow.of(1, 1, 1);
        datas[1] = GenericRow.of(2, 2, 2);
        String tableName = "format_table_test";
        sql(
                "CREATE TABLE %s (a INT, b INT, c INT) WITH ("
                        + "'file.format'='parquet',"
                        + "'type'='format-table',"
                        + "'format-table.implementation'='paimon'"
                        + ")",
                tableName);
        write(
                factory,
                new Path(dataPath, String.format("default.db/%s/data-1.parquet", tableName)),
                datas);
        RESTToken expiredDataToken =
                new RESTToken(
                        ImmutableMap.of(
                                "akId", "akId-expire", "akSecret", UUID.randomUUID().toString()),
                        System.currentTimeMillis() + 1000_000);
        Identifier identifier = Identifier.create("default", tableName);
        restCatalogServer.setDataToken(identifier, expiredDataToken);
        assertThat(sql("SELECT a FROM %s", tableName))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(2));
        assertThatThrownBy(() -> sql("INSERT INTO %s VALUES (3, 3, 3)", tableName))
                .isInstanceOf(RuntimeException.class);
        sql("Drop TABLE %s", tableName);
    }

    protected void write(FormatWriterFactory factory, Path file, InternalRow... rows)
            throws IOException {
        FileIO fileIO = new ResolvingFileIO();
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, dataPath);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        fileIO.configure(catalogContext);
        FormatWriter writer;
        PositionOutputStream out = null;
        if (factory instanceof SupportsDirectWrite) {
            writer = ((SupportsDirectWrite) factory).create(fileIO, file, "zstd");
        } else {
            out = fileIO.newOutputStream(file, true);
            writer = factory.create(out, "zstd");
        }
        for (InternalRow row : rows) {
            writer.addElement(row);
        }
        writer.close();
        if (out != null) {
            out.close();
        }
    }
}
