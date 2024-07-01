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

package org.apache.paimon.flink;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.lineage.DataLineageEntity;
import org.apache.paimon.lineage.DataLineageEntityImpl;
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.lineage.TableLineageEntity;
import org.apache.paimon.predicate.Predicate;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for flink table and data lineage. */
public class FlinkLineageITCase extends CatalogITCaseBase {
    private static final String THROWING_META = "throwing-meta";
    private static final Map<String, Map<String, TableLineageEntity>> jobSourceTableLineages =
            new HashMap<>();
    private static final Map<String, Map<String, TableLineageEntity>> jobSinkTableLineages =
            new HashMap<>();

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @Override
    protected Map<String, String> catalogOptions() {
        return Collections.singletonMap(LINEAGE_META.key(), THROWING_META);
    }

    @Test
    public void testTableLineage() throws Exception {
        // Validate for source and sink lineage when pipeline name is null
        assertThatThrownBy(
                        () -> tEnv.executeSql("INSERT INTO T VALUES (1, 2, 3),(4, 5, 6);").await())
                .hasCauseExactlyInstanceOf(ValidationException.class)
                .hasRootCauseMessage("Cannot get pipeline name for lineage meta.");
        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM T").collect().close())
                .hasCauseExactlyInstanceOf(ValidationException.class)
                .hasRootCauseMessage("Cannot get pipeline name for lineage meta.");

        // Call storeSinkTableLineage and storeSourceTableLineage methods
        tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, "insert_t_job");
        tEnv.executeSql("INSERT INTO T VALUES (1, 2, 3),(4, 5, 6);").await();
        assertThat(jobSinkTableLineages).isNotEmpty();
        TableLineageEntity sinkTableLineage =
                jobSinkTableLineages.get("insert_t_job").get("default.T.insert_t_job");
        assertThat(sinkTableLineage.getTable()).isEqualTo("T");

        List<Row> sinkTableRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql("SELECT * FROM sys.sink_table_lineage").collect()) {
            while (iterator.hasNext()) {
                sinkTableRows.add(iterator.next());
            }
        }
        assertThat(sinkTableRows.size()).isEqualTo(1);
        Row sinkTableRow = sinkTableRows.get(0);
        assertThat(sinkTableRow.getField("database_name")).isEqualTo("default");
        assertThat(sinkTableRow.getField("table_name")).isEqualTo("T");
        assertThat(sinkTableRow.getField("job_name")).isEqualTo("insert_t_job");

        tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, "select_t_job");
        tEnv.executeSql("SELECT * FROM T").collect().close();
        assertThat(jobSourceTableLineages).isNotEmpty();
        TableLineageEntity sourceTableLineage =
                jobSourceTableLineages.get("select_t_job").get("default.T.select_t_job");
        assertThat(sourceTableLineage.getTable()).isEqualTo("T");

        List<Row> sourceTableRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql("SELECT * FROM sys.source_table_lineage").collect()) {
            while (iterator.hasNext()) {
                sourceTableRows.add(iterator.next());
            }
        }
        assertThat(sourceTableRows.size()).isEqualTo(1);
        Row sourceTableRow = sourceTableRows.get(0);
        assertThat(sourceTableRow.getField("database_name")).isEqualTo("default");
        assertThat(sourceTableRow.getField("table_name")).isEqualTo("T");
        assertThat(sourceTableRow.getField("job_name")).isEqualTo("select_t_job");
    }

    @Test
    public void testDataLineage() throws Exception {

        List<Row> sinkTableRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql("SELECT * FROM sys.sink_data_lineage").collect()) {
            while (iterator.hasNext()) {
                sinkTableRows.add(iterator.next());
            }
        }
        assertThat(sinkTableRows.size()).isEqualTo(1);
        Row sinkTableRow = sinkTableRows.get(0);
        assertThat(sinkTableRow.getField("database_name")).isEqualTo("default");
        assertThat(sinkTableRow.getField("table_name")).isEqualTo("T");
        assertThat(sinkTableRow.getField("job_name")).isEqualTo("t_job");
        assertThat(sinkTableRow.getField("barrier_id")).isEqualTo(1L);
        assertThat(sinkTableRow.getField("snapshot_id")).isEqualTo(2L);

        List<Row> sourceTableRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator =
                tEnv.executeSql("SELECT * FROM sys.source_data_lineage").collect()) {
            while (iterator.hasNext()) {
                sourceTableRows.add(iterator.next());
            }
        }
        assertThat(sourceTableRows.size()).isEqualTo(1);
        Row sourceTableRow = sourceTableRows.get(0);
        assertThat(sourceTableRow.getField("database_name")).isEqualTo("default");
        assertThat(sourceTableRow.getField("table_name")).isEqualTo("T");
        assertThat(sourceTableRow.getField("job_name")).isEqualTo("t_job");
        assertThat(sinkTableRow.getField("barrier_id")).isEqualTo(1L);
        assertThat(sinkTableRow.getField("snapshot_id")).isEqualTo(2L);
    }

    private static String getTableLineageKey(TableLineageEntity entity) {
        return String.format("%s.%s.%s", entity.getDatabase(), entity.getTable(), entity.getJob());
    }

    /** Factory to create throwing lineage meta. */
    public static class TestingMemoryLineageMetaFactory implements LineageMetaFactory {
        private static final long serialVersionUID = 1L;

        @Override
        public String identifier() {
            return THROWING_META;
        }

        @Override
        public LineageMeta create(LineageMetaContext context) {
            return new TestingMemoryLineageMeta();
        }
    }

    /** Throwing specific exception in each method. */
    private static class TestingMemoryLineageMeta implements LineageMeta {

        DataLineageEntity dataLineageEntity =
                new DataLineageEntityImpl(
                        "default",
                        "T",
                        "t_job",
                        1,
                        2,
                        Timestamp.fromEpochMillis(System.currentTimeMillis()));

        @Override
        public void saveSourceTableLineage(TableLineageEntity entity) {
            jobSourceTableLineages
                    .computeIfAbsent(entity.getJob(), key -> new HashMap<>())
                    .put(getTableLineageKey(entity), entity);
        }

        @Override
        public void deleteSourceTableLineage(String job) {
            jobSourceTableLineages.remove(job);
        }

        @Override
        public Iterator<TableLineageEntity> sourceTableLineages(@Nullable Predicate predicate) {
            return jobSourceTableLineages.values().stream()
                    .flatMap(v -> v.values().stream())
                    .iterator();
        }

        @Override
        public void saveSinkTableLineage(TableLineageEntity entity) {
            assertThat(entity.getJob()).isEqualTo("insert_t_job");
            assertThat(entity.getTable()).isEqualTo("T");
            assertThat(entity.getDatabase()).isEqualTo("default");
            jobSinkTableLineages
                    .computeIfAbsent(entity.getJob(), key -> new HashMap<>())
                    .put(getTableLineageKey(entity), entity);
        }

        @Override
        public Iterator<TableLineageEntity> sinkTableLineages(@Nullable Predicate predicate) {
            return jobSinkTableLineages.values().stream()
                    .flatMap(v -> v.values().stream())
                    .iterator();
        }

        @Override
        public void deleteSinkTableLineage(String job) {
            jobSinkTableLineages.remove(job);
        }

        @Override
        public void saveSourceDataLineage(DataLineageEntity entity) {
            assertThat(entity.getJob()).isEqualTo("select_t_job");
            assertThat(entity.getTable()).isEqualTo("T");
            assertThat(entity.getDatabase()).isEqualTo("default");
            throw new UnsupportedOperationException("Method saveSinkTableLineage is not supported");
        }

        @Override
        public Iterator<DataLineageEntity> sourceDataLineages(@Nullable Predicate predicate) {
            return Collections.singletonList(dataLineageEntity).iterator();
        }

        @Override
        public void saveSinkDataLineage(DataLineageEntity entity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<DataLineageEntity> sinkDataLineages(@Nullable Predicate predicate) {
            return Collections.singletonList(dataLineageEntity).iterator();
        }

        @Override
        public void close() throws Exception {}
    }
}
