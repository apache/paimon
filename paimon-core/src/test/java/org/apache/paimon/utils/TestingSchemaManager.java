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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIOFinder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** {@link SchemaManager} subclass for testing. */
public class TestingSchemaManager extends SchemaManager {
    private final Map<Long, TableSchema> tableSchemas;

    public TestingSchemaManager(Path tableRoot, Map<Long, TableSchema> tableSchemas) {
        super(FileIOFinder.find(tableRoot), tableRoot);
        this.tableSchemas = tableSchemas;
    }

    @Override
    public Optional<TableSchema> latest() {
        return Optional.of(
                tableSchemas.get(
                        tableSchemas.keySet().stream()
                                .max(Long::compareTo)
                                .orElseThrow(IllegalStateException::new)));
    }

    @Override
    public List<TableSchema> listAll() {
        return new ArrayList<>(tableSchemas.values());
    }

    @Override
    public List<Long> listAllIds() {
        return new ArrayList<>(tableSchemas.keySet());
    }

    @Override
    public TableSchema createTable(Schema schema) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableSchema commitChanges(List<SchemaChange> changes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableSchema schema(long id) {
        return checkNotNull(tableSchemas.get(id));
    }
}
