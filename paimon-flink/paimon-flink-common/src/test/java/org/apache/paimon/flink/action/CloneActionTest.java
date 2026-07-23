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

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link CloneAction}. */
public class CloneActionTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDispatchPaimonFullHistoryCloneMode() throws Exception {
        Map<String, String> sourceCatalogConfig =
                Collections.singletonMap("warehouse", warehouse("source"));
        Map<String, String> targetCatalogConfig =
                Collections.singletonMap("warehouse", warehouse("target"));

        String sourceRoot;
        try (Catalog catalog =
                CatalogFactory.createCatalog(
                        CatalogContext.create(new Path(sourceCatalogConfig.get("warehouse"))))) {
            catalog.createDatabase("default", true);
            Identifier identifier = Identifier.create("default", "source_table");
            catalog.createTable(
                    identifier, Schema.newBuilder().column("id", DataTypes.INT()).build(), false);
            sourceRoot = ((FileStoreTable) catalog.getTable(identifier)).location().toString();
        }
        String targetRoot = new Path(tempDir.resolve("target-table").toUri()).toString();

        CloneAction action =
                new CloneAction(
                        "default",
                        "source_table",
                        sourceCatalogConfig,
                        null,
                        null,
                        targetCatalogConfig,
                        1,
                        null,
                        null,
                        null,
                        null,
                        "paimon",
                        "full-history",
                        Collections.singletonList(sourceRoot + "=" + targetRoot),
                        false,
                        true);

        assertThatNoException().isThrownBy(action::build);
        assertThat(action.env.getConfiguration().get(ExecutionOptions.RUNTIME_MODE))
                .isEqualTo(RuntimeExecutionMode.BATCH);
    }

    @Test
    public void testRejectUnknownPaimonCloneMode() {
        CloneAction action =
                new CloneAction(
                        "default",
                        "source_table",
                        Collections.singletonMap("warehouse", warehouse("source")),
                        "target_db",
                        "target_table",
                        Collections.singletonMap("warehouse", warehouse("target")),
                        1,
                        null,
                        null,
                        null,
                        null,
                        "paimon",
                        "unknown",
                        null,
                        false,
                        true);

        assertThatThrownBy(action::build)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported Paimon clone_mode");
    }

    @Test
    public void testRejectFullHistoryModeForHiveSource() {
        assertThatThrownBy(
                        () ->
                                new CloneAction(
                                        "default",
                                        "source_table",
                                        Collections.singletonMap("warehouse", warehouse("source")),
                                        "target_db",
                                        "target_table",
                                        Collections.singletonMap("warehouse", warehouse("target")),
                                        1,
                                        null,
                                        null,
                                        null,
                                        null,
                                        "hive",
                                        "full-history",
                                        Collections.emptyList(),
                                        false,
                                        true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("only supported when clone_from=paimon");
    }

    private String warehouse(String name) {
        java.nio.file.Path path = tempDir.resolve(name);
        return path.toUri().toString();
    }
}
