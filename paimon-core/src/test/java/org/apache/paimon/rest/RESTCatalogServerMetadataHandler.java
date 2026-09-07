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
import org.apache.paimon.rest.responses.ListSchemaResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.SchemaFilter;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

import okhttp3.mockwebserver.MockResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        return new MockResponse().setResponseCode(200).setBody(RESTApi.toJson(response));
    }

    static MockResponse listSchemas(FileStoreTable table, Map<String, String> parameters)
            throws Exception {
        SchemaManager schemaManager = new FileSystemSchemaManager(table.fileIO(), table.location());
        SchemaFilter filter = parseSchemaFilter(parameters);
        List<TableSchema> all = schemaManager.listAll();
        all.sort(Comparator.comparingLong(TableSchema::id).reversed());
        List<ListSchemaResponse.SchemaItem> items;
        if (filter.isLatest()) {
            items =
                    all.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(toSchemaItem(all.get(0)));
        } else if (filter.isEarliest()) {
            items =
                    all.isEmpty()
                            ? Collections.emptyList()
                            : Collections.singletonList(toSchemaItem(all.get(all.size() - 1)));
        } else if (filter.schemaId() != null) {
            long target = filter.schemaId();
            items =
                    all.stream()
                            .filter(s -> s.id() == target)
                            .findFirst()
                            .map(s -> Collections.singletonList(toSchemaItem(s)))
                            .orElse(Collections.emptyList());
        } else {
            items =
                    all.stream()
                            .filter(
                                    s ->
                                            filter.maxSchemaId() == null
                                                    || s.id() <= filter.maxSchemaId())
                            .filter(
                                    s ->
                                            filter.minSchemaId() == null
                                                    || s.id() >= filter.minSchemaId())
                            .map(RESTCatalogServerMetadataHandler::toSchemaItem)
                            .collect(Collectors.toList());
        }
        ListSchemaResponse response = new ListSchemaResponse(items);
        return new MockResponse().setResponseCode(200).setBody(RESTApi.toJson(response));
    }

    private static SchemaFilter parseSchemaFilter(Map<String, String> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return SchemaFilter.all();
        }
        if ("true".equalsIgnoreCase(parameters.get("latest"))) {
            return SchemaFilter.latest();
        }
        if ("true".equalsIgnoreCase(parameters.get("earliest"))) {
            return SchemaFilter.earliest();
        }
        String schemaId = parameters.get("schemaId");
        if (schemaId != null) {
            return SchemaFilter.withId(Long.parseLong(schemaId));
        }
        String maxSchemaId = parameters.get("maxSchemaId");
        String minSchemaId = parameters.get("minSchemaId");
        Long max = maxSchemaId == null ? null : Long.parseLong(maxSchemaId);
        Long min = minSchemaId == null ? null : Long.parseLong(minSchemaId);
        if (max == null && min == null) {
            return SchemaFilter.all();
        }
        return SchemaFilter.range(max, min);
    }

    private static ListSchemaResponse.SchemaItem toSchemaItem(TableSchema schema) {
        return new ListSchemaResponse.SchemaItem(
                schema.id(), schema.toSchema(), schema.timeMillis());
    }
}
