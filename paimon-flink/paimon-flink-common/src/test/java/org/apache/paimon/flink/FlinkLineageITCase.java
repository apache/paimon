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

import org.apache.paimon.lineage.DataLineageEntity;
import org.apache.paimon.lineage.LineageMeta;
import org.apache.paimon.lineage.LineageMetaFactory;
import org.apache.paimon.lineage.TableLineageEntity;
import org.apache.paimon.predicate.Predicate;

import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.LINEAGE_META;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** ITCase for flink table and data lineage. */
public class FlinkLineageITCase extends CatalogITCaseBase {
    private static final String THROWING_META = "throwing-meta";

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @Override
    protected Map<String, String> catalogOptions() {
        return Collections.singletonMap(LINEAGE_META.key(), THROWING_META);
    }

    @Test
    public void testTableLineage() {
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
        assertThatThrownBy(
                        () -> tEnv.executeSql("INSERT INTO T VALUES (1, 2, 3),(4, 5, 6);").await())
                .hasCauseExactlyInstanceOf(UnsupportedOperationException.class)
                .hasRootCauseMessage("Method storeSinkTableLineage is not supported");

        tEnv.getConfig().getConfiguration().set(PipelineOptions.NAME, "select_t_job");
        assertThatThrownBy(() -> tEnv.executeSql("SELECT * FROM T").collect().close())
                .hasCauseExactlyInstanceOf(UnsupportedOperationException.class)
                .hasRootCauseMessage("Method storeSourceTableLineage is not supported");
    }

    /** Factory to create throwing lineage meta. */
    public static class ThrowingLineageMetaFactory implements LineageMetaFactory {
        @Override
        public String identifier() {
            return THROWING_META;
        }

        @Override
        public LineageMeta create(LineageMetaContext context) {
            return new ThrowingLineageMeta();
        }
    }

    /** Throwing specific exception in each method. */
    private static class ThrowingLineageMeta implements LineageMeta {

        private static final long serialVersionUID = 1L;

        @Override
        public void storeSourceTableLineage(TableLineageEntity entity) {
            throw new UnsupportedOperationException(
                    "Method storeSourceTableLineage is not supported");
        }

        @Override
        public void deleteSourceTableLineage(String job) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<TableLineageEntity> sourceTableLineages(@Nullable Predicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeSinkTableLineage(TableLineageEntity entity) {
            assertEquals("insert_t_job", entity.getJob());
            assertEquals("T", entity.getTable());
            assertEquals("default", entity.getDatabase());
            throw new UnsupportedOperationException(
                    "Method storeSinkTableLineage is not supported");
        }

        @Override
        public Iterator<TableLineageEntity> sinkTableLineages(@Nullable Predicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteSinkTableLineage(String job) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeSourceDataLineage(DataLineageEntity entity) {
            assertEquals("select_t_job", entity.getJob());
            assertEquals("T", entity.getTable());
            assertEquals("default", entity.getDatabase());
            throw new UnsupportedOperationException(
                    "Method storeSinkTableLineage is not supported");
        }

        @Override
        public Iterator<DataLineageEntity> sourceDataLineages(@Nullable Predicate predicate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void storeSinkDataLineage(DataLineageEntity entity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<DataLineageEntity> sinkDataLineages(@Nullable Predicate predicate) {
            throw new UnsupportedOperationException();
        }
    }
}
