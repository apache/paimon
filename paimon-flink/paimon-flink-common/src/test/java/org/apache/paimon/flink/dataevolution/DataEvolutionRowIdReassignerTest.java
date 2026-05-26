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

package org.apache.paimon.flink.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.ReassignRowIdAction;
import org.apache.paimon.flink.action.ReassignRowIdActionFactory;
import org.apache.paimon.flink.procedure.ReassignRowIdProcedure;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReassignRowIdAction} and {@link ReassignRowIdProcedure}. */
public class DataEvolutionRowIdReassignerTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("pt", DataTypes.STRING());
        schemaBuilder.column("id", DataTypes.INT());
        schemaBuilder.column("payload", DataTypes.STRING());
        schemaBuilder.partitionKeys("pt");
        schemaBuilder.option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.GLOBAL_INDEX_ENABLED.key(), "true");
        schemaBuilder.option(CoreOptions.BUCKET.key(), "-1");
        schemaBuilder.option(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "1");
        return schemaBuilder.build();
    }

    @Test
    public void testActionReassignRowIdsByPartition() throws Exception {
        FileStoreTable table = createTableWithInterleavedPartitions();

        Action action =
                ActionFactory.createAction(
                                new String[] {
                                    ReassignRowIdActionFactory.IDENTIFIER,
                                    "--warehouse",
                                    warehouse.toString(),
                                    "--database",
                                    database,
                                    "--table",
                                    DEFAULT_TABLE_NAME
                                })
                        .get();
        assertThat(action).isInstanceOf(ReassignRowIdAction.class);

        action.run();

        Map<String, List<Long>> rowIdsByPartition = rowIdsByPartition(table);
        assertThat(rowIdsByPartition).hasSize(2);
        assertThat(rowIdsByPartition).containsEntry("pt=a/", Arrays.asList(5L, 6L, 7L));
        assertThat(rowIdsByPartition).containsEntry("pt=b/", Arrays.asList(8L, 9L));
        assertThat(table.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(10L);
    }

    @Test
    public void testActionBuildsBoundedSourceWithSink() throws Exception {
        createTableDefault();
        ReassignRowIdAction action =
                (ReassignRowIdAction)
                        ActionFactory.createAction(
                                        new String[] {
                                            ReassignRowIdActionFactory.IDENTIFIER,
                                            "--warehouse",
                                            warehouse.toString(),
                                            "--database",
                                            database,
                                            "--table",
                                            DEFAULT_TABLE_NAME
                                        })
                                .get();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        action.withStreamExecutionEnvironment(env).build();

        List<Transformation<?>> transformations = env.getTransformations();
        assertThat(transformations).isNotEmpty();
        String sourceName = "Reassign Row ID : " + database + "." + DEFAULT_TABLE_NAME;
        Transformation<?> source =
                transformations.stream()
                        .map(transformation -> findTransformationByName(transformation, sourceName))
                        .filter(Objects::nonNull)
                        .findFirst()
                        .orElse(null);
        assertThat(source).isNotNull();
        assertThat(source).isInstanceOf(LegacySourceTransformation.class);
        assertThat(((LegacySourceTransformation<?>) source).getBoundedness())
                .isEqualTo(Boundedness.BOUNDED);
        assertThat(source.getParallelism()).isEqualTo(1);
        assertThat(source.getInputs()).isEmpty();

        Transformation<?> sink =
                transformations.stream()
                        .filter(transformation -> transformation != source)
                        .filter(
                                transformation ->
                                        findTransformationByName(transformation, sourceName)
                                                == source)
                        .findFirst()
                        .orElse(null);
        assertThat(sink).isNotNull();
        assertThat(sink.getParallelism()).isEqualTo(1);
        assertThat(findTransformationByName(sink, sourceName)).isSameAs(source);
        assertThat(env.getStreamGraph().getJobGraph().getVertices()).hasSize(2);
    }

    @Test
    public void testProcedureReassignsSelectedPartition() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "c", 2);
        writeOneRow(table, "c", 3);
        writeOneRow(table, "d", 4);
        writeOneRow(table, "d", 5);
        writeOneRow(table, "b", 6);

        ReassignRowIdProcedure procedure = new ReassignRowIdProcedure();
        procedure.withCatalog(catalog);
        String[] result = procedure.call(null, database + "." + DEFAULT_TABLE_NAME, "pt=b");

        assertThat(result).hasSize(1);
        assertThat(result[0]).contains("Success. Reassigned row IDs");
        assertThat(rowIdsByPartition(table))
                .containsEntry("pt=a/", Collections.singletonList(0L))
                .containsEntry("pt=b/", Arrays.asList(7L, 8L))
                .containsEntry("pt=c/", Arrays.asList(2L, 3L))
                .containsEntry("pt=d/", Arrays.asList(4L, 5L));
    }

    private Transformation<?> findTransformationByName(
            Transformation<?> transformation, String expectedName) {
        if (expectedName.equals(transformation.getName())) {
            return transformation;
        }
        for (Transformation<?> input : transformation.getInputs()) {
            Transformation<?> found = findTransformationByName(input, expectedName);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private FileStoreTable createTableWithInterleavedPartitions() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        writeOneRow(table, "a", 0);
        writeOneRow(table, "b", 1);
        writeOneRow(table, "a", 2);
        writeOneRow(table, "b", 3);
        writeOneRow(table, "a", 4);
        return table;
    }

    private void writeOneRow(FileStoreTable table, String partition, int id) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            write.write(
                    GenericRow.of(
                            BinaryString.fromString(partition),
                            id,
                            BinaryString.fromString("v" + id)));
            commit.commit(write.prepareCommit());
        }
    }

    private Map<String, List<Long>> rowIdsByPartition(FileStoreTable table) {
        List<ManifestEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(table.snapshotManager().latestSnapshot())
                        .plan()
                        .files();
        Map<String, List<Long>> result = new LinkedHashMap<>();
        for (ManifestEntry entry : entries) {
            String partition = table.store().pathFactory().getPartitionString(entry.partition());
            result.computeIfAbsent(partition, k -> new ArrayList<>())
                    .add(entry.file().firstRowId());
        }
        for (List<Long> rowIds : result.values()) {
            Collections.sort(rowIds);
        }
        return result;
    }
}
