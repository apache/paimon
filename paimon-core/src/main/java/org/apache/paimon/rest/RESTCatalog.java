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

package org.apache.paimon.rest;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.rest.requests.ConfigRequest;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Map;

public class RESTCatalog implements Catalog {
    private RESTClient client;

    public RESTCatalog(String endpoint) {
        this.client = new HttpClient(endpoint);
    }

    @Override
    public String warehouse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> options() {
        ConfigResponse response =
                RestClientUtil.getResponse(client.getClient().getConfig(new ConfigRequest()))
                        .body();
        return response.getDefaults();
    }

    @Override
    public FileIO fileIO() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listDatabases() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Database getDatabase(String name) throws DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getTableLocation(Identifier identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(Identifier identifier, Map<String, String> partitionSpec)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(Identifier identifier, Map<String, String> partitions)
            throws TableNotExistException, PartitionNotExistException {}

    @Override
    public List<PartitionEntry> listPartitions(Identifier identifier)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean allowUpperCase() {
        return false;
    }

    @Override
    public void close() throws Exception {}
}
