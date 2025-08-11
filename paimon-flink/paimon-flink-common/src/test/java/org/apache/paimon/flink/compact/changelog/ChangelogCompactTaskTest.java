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

package org.apache.paimon.flink.compact.changelog;

import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ChangelogCompactTask}. */
public class ChangelogCompactTaskTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testExceptionWhenRead() throws Exception {
        FileSystemCatalog catalog =
                new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toString()));
        catalog.createDatabase("default", false);
        catalog.createTable(
                Identifier.create("default", "T"),
                new Schema(
                        Arrays.asList(
                                new DataField(0, "k", DataTypes.INT()),
                                new DataField(1, "v", DataTypes.INT())),
                        Collections.emptyList(),
                        Collections.singletonList("k"),
                        new HashMap<>(),
                        ""),
                false);

        Map<Integer, List<DataFileMeta>> files = new HashMap<>();
        files.put(
                0,
                Collections.singletonList(
                        DataFileMeta.forAppend(
                                "unexisting-file",
                                128,
                                0,
                                SimpleStats.EMPTY_STATS,
                                0,
                                0,
                                1,
                                Collections.emptyList(),
                                null,
                                null,
                                null,
                                null,
                                null,
                                null)));
        ChangelogCompactTask task =
                new ChangelogCompactTask(1, BinaryRow.EMPTY_ROW, 1, files, new HashMap<>());
        assertThatThrownBy(
                        () ->
                                task.doCompact(
                                        (FileStoreTable)
                                                catalog.getTable(Identifier.create("default", "T")),
                                        Executors.newFixedThreadPool(1),
                                        MemorySize.ofMebiBytes(64)))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("unexisting-file");
    }
}
