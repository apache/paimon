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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Scan-local cache for data evolution stats projections.
 *
 * <p>This class is not thread-safe. It is only accessed from the single-threaded post-filter phase
 * of {@link AbstractFileStoreScan#plan()}.
 */
class EvolutionStatsCache {

    private final Map<CacheKey, ProjectedFileSchema> cache = new HashMap<>();

    ProjectedFileSchema get(Function<Long, TableSchema> scanTableSchema, DataFileMeta fileMeta) {
        CacheKey key =
                new CacheKey(fileMeta.schemaId(), fileMeta.writeCols(), fileMeta.valueStatsCols());
        return cache.computeIfAbsent(key, ignored -> projectFileSchema(scanTableSchema, key));
    }

    @VisibleForTesting
    int size() {
        return cache.size();
    }

    private static ProjectedFileSchema projectFileSchema(
            Function<Long, TableSchema> scanTableSchema, CacheKey key) {
        TableSchema dataFileSchema =
                scanTableSchema.apply(key.schemaId).dataFileSchema(key.writeColumns);
        TableSchema dataFileSchemaWithStats = dataFileSchema.project(key.valueStatsColumns);
        List<DataField> fields = dataFileSchema.fields();
        Map<Integer, FileFieldStats> fieldStats = new HashMap<>(fields.size() * 2);
        for (DataField field : fields) {
            fieldStats.put(field.id(), FileFieldStats.withoutStats());
        }
        List<DataField> statsFields = dataFileSchemaWithStats.fields();
        for (int i = 0; i < statsFields.size(); i++) {
            DataField statsField = statsFields.get(i);
            fieldStats.put(statsField.id(), FileFieldStats.withStats(i, statsField.type()));
        }
        return new ProjectedFileSchema(dataFileSchema, fieldStats);
    }

    static class ProjectedFileSchema {

        private final TableSchema dataFileSchema;
        private final Map<Integer, FileFieldStats> fieldStats;

        private ProjectedFileSchema(
                TableSchema dataFileSchema, Map<Integer, FileFieldStats> fieldStats) {
            this.dataFileSchema = dataFileSchema;
            this.fieldStats = fieldStats;
        }

        TableSchema dataFileSchema() {
            return dataFileSchema;
        }

        @Nullable
        FileFieldStats fieldStats(int fieldId) {
            return fieldStats.get(fieldId);
        }
    }

    static class FileFieldStats {

        private static final FileFieldStats WITHOUT_STATS = new FileFieldStats(0, null);

        private final int index;
        @Nullable private final DataType type;

        private FileFieldStats(int index, @Nullable DataType type) {
            this.index = index;
            this.type = type;
        }

        static FileFieldStats withoutStats() {
            return WITHOUT_STATS;
        }

        static FileFieldStats withStats(int index, DataType type) {
            return new FileFieldStats(index, type);
        }

        boolean hasStats() {
            return type != null;
        }

        int index() {
            checkNotNull(type, "Stats index is unavailable for a field without stats.");
            return index;
        }

        DataType type() {
            return checkNotNull(type, "Stats type is unavailable for a field without stats.");
        }
    }

    private static class CacheKey {

        private final long schemaId;
        private final List<String> writeColumns;
        private final List<String> valueStatsColumns;

        private CacheKey(long schemaId, List<String> writeColumns, List<String> valueStatsColumns) {
            this.schemaId = schemaId;
            this.writeColumns = writeColumns;
            this.valueStatsColumns = valueStatsColumns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return schemaId == cacheKey.schemaId
                    && Objects.equals(writeColumns, cacheKey.writeColumns)
                    && Objects.equals(valueStatsColumns, cacheKey.valueStatsColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schemaId, writeColumns, valueStatsColumns);
        }
    }
}
