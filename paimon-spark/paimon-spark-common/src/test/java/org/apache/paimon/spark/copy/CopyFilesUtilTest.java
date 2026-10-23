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

package org.apache.paimon.spark.copy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CopyFilesUtil}. */
public class CopyFilesUtilTest {

    @Test
    void testNewDataFilePathKeepsTheKindOfTheFile() {
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path("/target/bucket-0"),
                        "parquet",
                        "data-",
                        "changelog-",
                        false,
                        null,
                        null);

        assertThat(newName(pathFactory, "data-a-0.parquet"))
                .endsWith(".parquet")
                .doesNotContain(".vector.");
        assertThat(newName(pathFactory, "data-a-0.vector.lance")).endsWith(".vector.lance");
        assertThat(newName(pathFactory, "data-a-0.blob")).endsWith(".blob");
        assertThat(newName(pathFactory, "data-a-0.video")).endsWith(".video");
    }

    private static String newName(DataFilePathFactory pathFactory, String sourceFileName) {
        String name = CopyFilesUtil.newDataFilePath(pathFactory, sourceFileName).getName();
        assertThat(name).startsWith("data-").doesNotStartWith("data-a-");
        return name;
    }

    @Test
    void testClearWriteColsSequencesWhenChangingSchemaId() {
        DataFileMeta source =
                DataFileMeta.forAppend(
                                "source.parquet",
                                10L,
                                2L,
                                SimpleStats.EMPTY_STATS,
                                1L,
                                3L,
                                5L,
                                Collections.emptyList(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                Arrays.asList("a", "b"))
                        .withWriteColsSequences(new long[] {2L, 3L});

        DataFileMeta copied = CopyFilesUtil.toNewDataFileMeta(source, "copied.parquet", 6L);

        assertThat(copied.fileName()).isEqualTo("copied.parquet");
        assertThat(copied.schemaId()).isEqualTo(6L);
        assertThat(copied.writeCols()).containsExactly("a", "b");
        assertThat(copied.writeColsSequences()).isNull();
    }

    @Test
    void testMaterializeCompactWriteColsWhenChangingSchemaId() {
        DataFileMeta source =
                DataFileMeta.forAppend(
                        "source.parquet",
                        10L,
                        2L,
                        SimpleStats.EMPTY_STATS,
                        1L,
                        3L,
                        5L,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        TableSchema sourceSchema =
                TableSchema.create(
                        5L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("blob", DataTypes.BLOB())
                                .column("name", DataTypes.STRING())
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .option(
                                        CoreOptions.DATA_EVOLUTION_WRITE_COLS_OPTIMIZATION_ENABLED
                                                .key(),
                                        "true")
                                .build());

        DataFileMeta copied =
                CopyFilesUtil.toNewDataFileMeta(source, "copied.parquet", 9L, sourceSchema);

        assertThat(copied.schemaId()).isEqualTo(9L);
        assertThat(copied.writeCols()).containsExactly("id", "name");
    }

    @Test
    void testMaterializeLegacyFullSchemaWriteColsWhenChangingSchemaId() {
        DataFileMeta source =
                DataFileMeta.forAppend(
                        "source.parquet",
                        10L,
                        2L,
                        SimpleStats.EMPTY_STATS,
                        1L,
                        3L,
                        5L,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        TableSchema sourceSchema =
                TableSchema.create(
                        5L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("blob", DataTypes.BLOB())
                                .column("name", DataTypes.STRING())
                                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                                .build());

        DataFileMeta copied =
                CopyFilesUtil.toNewDataFileMeta(source, "copied.parquet", 9L, sourceSchema);

        assertThat(copied.schemaId()).isEqualTo(9L);
        assertThat(copied.writeCols()).containsExactly("id", "blob", "name");
    }
}
