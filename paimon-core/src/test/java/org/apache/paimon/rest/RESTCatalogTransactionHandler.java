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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CommitTransactionRequest;
import org.apache.paimon.rest.requests.CommitTransactionRequest.TableChange;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.table.TableSnapshot;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.List;
import java.util.Objects;

import static org.apache.paimon.rest.RESTCatalogServerUtils.parseRequest;

/** Atomic transaction endpoint used by {@link RESTCatalogServer}. */
final class RESTCatalogTransactionHandler {

    static volatile boolean commitConflict = false;
    static volatile boolean commitSuccessThrowException = false;
    static volatile boolean badRequest = false;
    static volatile String missingTable = null;
    static volatile String forbiddenTable = null;

    private RESTCatalogTransactionHandler() {}

    static boolean matches(RecordedRequest request, ResourcePaths resourcePaths) {
        return "POST".equals(request.getMethod())
                && resourcePaths.commitTransaction().equals(request.getPath());
    }

    static MockResponse handle(RESTCatalogServer server, String data) throws Exception {
        synchronized (server) {
            return handleLocked(server, data);
        }
    }

    private static MockResponse handleLocked(RESTCatalogServer server, String data)
            throws Exception {
        CommitTransactionRequest request = parseRequest(data, CommitTransactionRequest.class);
        List<TableChange> tableChanges = request.getTableChanges();
        if (tableChanges == null || tableChanges.isEmpty()) {
            return server.mockResponse(
                    new ErrorResponse(null, null, "Empty transaction", 400), 400);
        }
        if (badRequest) {
            badRequest = false;
            return server.mockResponse(
                    new ErrorResponse(null, null, "Invalid transaction", 400), 400);
        }

        for (TableChange tableChange : tableChanges) {
            Identifier identifier = tableChange.getIdentifier();
            CommitTableRequest commit = tableChange.getCommit();
            if (identifier == null || commit == null) {
                return server.mockResponse(
                        new ErrorResponse(null, null, "Invalid table change", 400), 400);
            }
            if (Objects.equals(missingTable, identifier.getFullName())) {
                missingTable = null;
                throw new Catalog.TableNotExistException(identifier);
            }
            if (Objects.equals(forbiddenTable, identifier.getFullName())) {
                forbiddenTable = null;
                throw new Catalog.TableNoPermissionException(identifier);
            }
            if (server.noPermissionTables.contains(identifier.getFullName())) {
                throw new Catalog.TableNoPermissionException(identifier);
            }
            TableMetadata metadata = server.tableMetadataStore.get(identifier.getFullName());
            if (metadata == null || !Objects.equals(metadata.uuid(), commit.getTableId())) {
                throw new Catalog.TableNotExistException(identifier);
            }
            TableSnapshot currentSnapshot =
                    server.tableLatestSnapshotStore.get(identifier.getFullName());
            String currentSnapshotUuid =
                    currentSnapshot == null ? null : currentSnapshot.snapshot().uuid();
            if (!Objects.equals(currentSnapshotUuid, commit.getBaseSnapshotUuid())) {
                return conflictResponse(server);
            }
        }

        if (commitConflict) {
            commitConflict = false;
            return conflictResponse(server);
        }

        boolean singleCommitFailure = RESTCatalogServer.commitSuccessThrowException;
        RESTCatalogServer.commitSuccessThrowException = false;
        try {
            for (TableChange tableChange : tableChanges) {
                CommitTableRequest commit = tableChange.getCommit();
                server.commitSnapshot(
                        tableChange.getIdentifier(),
                        commit.getTableId(),
                        commit.getBaseSnapshotUuid(),
                        commit.getSnapshot(),
                        commit.getStatistics());
            }
        } finally {
            RESTCatalogServer.commitSuccessThrowException = singleCommitFailure;
        }

        if (commitSuccessThrowException) {
            commitSuccessThrowException = false;
            return server.mockResponse(
                    new ErrorResponse(null, null, "Unknown transaction commit state", 503), 503);
        }
        return new MockResponse().setResponseCode(204);
    }

    private static MockResponse conflictResponse(RESTCatalogServer server) {
        return server.mockResponse(
                new ErrorResponse(null, null, "Transaction commit conflict", 409), 409);
    }
}
