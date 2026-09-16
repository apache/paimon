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
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CopyFilesUtil}. */
public class CopyFilesUtilTest {

    @Test
    void testClearColumnSequencesWhenChangingSchemaId() {
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
                        .withColumnMaxSequenceNumbers(new long[] {2L, 3L});

        DataFileMeta copied = CopyFilesUtil.toNewDataFileMeta(source, "copied.parquet", 6L);

        assertThat(copied.fileName()).isEqualTo("copied.parquet");
        assertThat(copied.schemaId()).isEqualTo(6L);
        assertThat(copied.writeCols()).containsExactly("a", "b");
        assertThat(copied.columnMaxSequenceNumbers()).isNull();
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

    @Test
    void testPreserveGlobalIndexMetaAndRebindSchemaId() {
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(0L, 9L, 1, null, new byte[] {1, 2}, new byte[] {3, 4});
        IndexFileMeta source =
                new IndexFileMeta(
                        "btree", "source.idx", 100L, 10L, null, null, globalIndexMeta, 5L);

        IndexFileMeta copied = CopyFilesUtil.toNewIndexFileMeta(source, "copied.idx", 8L);

        assertThat(copied.fileName()).isEqualTo("copied.idx");
        assertThat(copied.globalIndexMeta()).isEqualTo(globalIndexMeta);
        assertThat(copied.schemaId()).isEqualTo(8L);
    }

    @Test
    void testRemapPrimaryKeyIndexSourceFiles() {
        PrimaryKeyIndexSourceMeta sourceMeta =
                new PrimaryKeyIndexSourceMeta(
                        1,
                        Arrays.asList(
                                new PrimaryKeyIndexSourceFile("data-1.parquet", 2L),
                                new PrimaryKeyIndexSourceFile("data-2.parquet", 3L)));
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(0L, 4L, 1, null, null, sourceMeta.serialize());
        IndexFileMeta source =
                new IndexFileMeta(
                        "btree", "source.idx", 100L, 5L, null, null, globalIndexMeta, null);
        Map<String, String> dataFileNameMapping = new HashMap<>();
        dataFileNameMapping.put("data-1.parquet", "copied-1.parquet");
        dataFileNameMapping.put("data-2.parquet", "copied-2.parquet");

        IndexFileMeta copied =
                CopyFilesUtil.toNewPrimaryKeyIndexFileMeta(
                        source, "copied.idx", dataFileNameMapping);

        assertThat(PrimaryKeyIndexSourceMeta.fromIndexFile(copied).sourceFiles())
                .containsExactly(
                        new PrimaryKeyIndexSourceFile("copied-1.parquet", 2L),
                        new PrimaryKeyIndexSourceFile("copied-2.parquet", 3L));
        assertThat(copied.schemaId()).isNull();
    }
}
