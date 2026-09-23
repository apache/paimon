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

import org.apache.paimon.Snapshot;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetSchemaResponse;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.rest.responses.ListSchemasResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.SnapshotManager;

import okhttp3.mockwebserver.MockResponse;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Metadata response handlers used by {@link RESTCatalogServer}. */
final class RESTCatalogServerMetadataHandler {

    private RESTCatalogServerMetadataHandler() {}

    static MockResponse listSnapshots(FileStoreTable table) throws Exception {
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        List<Snapshot> snapshotList = new ArrayList<>();
        while (snapshots.hasNext()) {
            snapshotList.add(snapshots.next());
        }
        ListSnapshotsResponse response = new ListSnapshotsResponse(snapshotList, null);
        return response(response);
    }

    static MockResponse loadSnapshot(FileStoreTable table, String version) throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = null;
        try {
            if (version.equals("EARLIEST")) {
                snapshot = snapshotManager.earliestSnapshot();
            } else if (version.equals("LATEST")) {
                snapshot = snapshotManager.latestSnapshot();
            } else {
                try {
                    snapshot = snapshotManager.tryGetSnapshot(Long.parseLong(version));
                } catch (NumberFormatException e) {
                    Optional<Tag> tag = table.tagManager().get(version);
                    if (tag.isPresent()) {
                        snapshot = tag.get().trimToSnapshot();
                    }
                }
            }
        } catch (Exception ignored) {
        }

        if (snapshot == null) {
            return notFound(ErrorResponse.RESOURCE_TYPE_SNAPSHOT, "No Snapshot");
        }
        return response(new GetVersionSnapshotResponse(snapshot));
    }

    static MockResponse loadSchema(FileStoreTable table, String version) throws Exception {
        SchemaManager schemaManager = schemaManager(table);
        TableSchema schema = null;
        if ("LATEST".equals(version)) {
            schema = schemaManager.latest().orElse(null);
        } else {
            List<TableSchema> schemas = schemaManager.listAll();
            if ("EARLIEST".equals(version)) {
                schema =
                        schemas.stream()
                                .min(Comparator.comparingLong(TableSchema::id))
                                .orElse(null);
            } else {
                try {
                    long schemaId = Long.parseLong(version);
                    if (schemaManager.schemaExists(schemaId)) {
                        schema = schemaManager.schema(schemaId);
                    }
                } catch (NumberFormatException ignored) {
                }
            }
        }

        if (schema == null) {
            return notFound(ErrorResponse.RESOURCE_TYPE_SCHEMA, "No Schema");
        }
        return response(new GetSchemaResponse(schema));
    }

    static MockResponse listSchemas(FileStoreTable table, int maxResults, String pageToken)
            throws Exception {
        List<TableSchema> schemas = schemaManager(table).listAll();
        schemas.sort(Comparator.comparingLong(TableSchema::id).reversed());
        if (pageToken != null) {
            long previousSchemaId = Long.parseLong(pageToken);
            schemas =
                    schemas.stream()
                            .filter(schema -> schema.id() < previousSchemaId)
                            .collect(Collectors.toList());
        }

        int resultSize = Math.min(maxResults, schemas.size());
        List<TableSchema> result = new ArrayList<>(schemas.subList(0, resultSize));
        String nextPageToken =
                resultSize < schemas.size()
                        ? Long.toString(result.get(result.size() - 1).id())
                        : null;
        return response(new ListSchemasResponse(result, nextPageToken));
    }

    private static SchemaManager schemaManager(FileStoreTable table) {
        return new FileSystemSchemaManager(table.fileIO(), table.location());
    }

    private static MockResponse notFound(String resourceType, String message) throws Exception {
        return new MockResponse()
                .setResponseCode(404)
                .setBody(RESTApi.toJson(new ErrorResponse(resourceType, null, message, 404)));
    }

    private static MockResponse response(RESTResponse response) throws Exception {
        return new MockResponse().setResponseCode(200).setBody(RESTApi.toJson(response));
    }
}
