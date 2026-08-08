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

package org.apache.paimon.service;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/** A manager to manage services, for example, the service to lookup row from the primary key. */
public class ServiceManager implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SERVICE_PREFIX = "service-";

    public static final String PRIMARY_KEY_LOOKUP = "primary-key-lookup";

    public static final String GLOBAL_INDEX_LOOKUP = "global-index-lookup";

    /** A schema-specific service ID prevents clients from decoding a different projection. */
    public static String globalIndexLookupService(
            long schemaId, int lookupFieldId, int[] valueFieldIds) {
        StringBuilder builder =
                new StringBuilder("v")
                        .append(GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION)
                        .append("-s")
                        .append(schemaId)
                        .append("-k")
                        .append(lookupFieldId)
                        .append("-v");
        for (int fieldId : valueFieldIds) {
            builder.append(fieldId).append('-');
        }
        String canonical = builder.substring(0, builder.length() - 1);
        // Service IDs become file names. Hash the complete ordered projection so wide tables can
        // never exceed common 255-byte path-component limits; the descriptor retains all IDs for
        // collision-safe schema validation.
        return GLOBAL_INDEX_LOOKUP
                + "-v"
                + GlobalIndexQueryServiceDescriptor.PROTOCOL_VERSION
                + '-'
                + UUID.nameUUIDFromBytes(canonical.getBytes(StandardCharsets.UTF_8));
    }

    private final FileIO fileIO;
    private final Path tablePath;

    public ServiceManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    public Path tablePath() {
        return tablePath;
    }

    public Optional<InetSocketAddress[]> service(String id) {
        try {
            return fileIO.readOverwrittenFileUtf8(servicePath(id))
                    .map(s -> JsonSerdeUtil.fromJson(s, InetSocketAddress[].class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void resetService(String id, InetSocketAddress[] addresses) {
        try {
            fileIO.overwriteFileUtf8(servicePath(id), JsonSerdeUtil.toJson(addresses));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Optional<GlobalIndexQueryServiceDescriptor> globalIndexService(String id) {
        try {
            return selectGlobalIndexService(id);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the next persisted publisher sequence for one logical query-service job. Independent
     * jobs must not concurrently publish the same service because file systems do not provide a
     * compare-and-set primitive for allocating this sequence.
     */
    public long nextGlobalIndexOwnerSequence(String id) {
        try {
            long maximum = -1L;
            Path directory = serviceDirectory();
            if (fileIO.exists(directory)) {
                String prefix = ownerServicePrefix(id);
                for (FileStatus status : fileIO.listStatus(directory)) {
                    if (status.isDir() || !status.getPath().getName().startsWith(prefix)) {
                        continue;
                    }
                    Optional<GlobalIndexQueryServiceDescriptor> descriptor =
                            readGlobalIndexDescriptor(status.getPath());
                    if (descriptor.isPresent()) {
                        maximum = Math.max(maximum, ownerSequence(descriptor.get().ownerToken()));
                    }
                }
            }
            Optional<GlobalIndexQueryServiceDescriptor> legacy =
                    readGlobalIndexDescriptor(servicePath(id));
            if (legacy.isPresent()) {
                maximum = Math.max(maximum, ownerSequence(legacy.get().ownerToken()));
            }
            if (maximum == Long.MAX_VALUE) {
                throw new IllegalStateException("Global-index service owner sequence overflow.");
            }
            return maximum + 1L;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void resetGlobalIndexService(String id, GlobalIndexQueryServiceDescriptor descriptor) {
        try {
            fileIO.overwriteFileUtf8(
                    ownerServicePath(id, descriptor.ownerToken()),
                    JsonSerdeUtil.toJson(descriptor));
            // Once an owner-scoped publisher has claimed the service, an old canonical descriptor
            // must never become visible again after that publisher closes.
            fileIO.deleteQuietly(servicePath(id));
            deleteOlderOwnerServices(id, descriptor.ownerToken());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Owner-aware cleanup. Each publisher writes an independent descriptor file, so an old attempt
     * can neither overwrite nor delete a newer attempt. The current owner leaves a not-ready
     * tombstone on close so a delayed old write cannot expose an old ready descriptor again.
     */
    public void deleteGlobalIndexServiceIfOwned(String id, String ownerToken) {
        try {
            // Read the deterministic owner path first. Closing must not depend on a directory
            // listing observing the current file; otherwise a temporarily omitted READY file can
            // reappear after close without a tombstone.
            Optional<GlobalIndexQueryServiceDescriptor> ownDescriptor =
                    readGlobalIndexDescriptor(ownerServicePath(id, ownerToken));
            Optional<GlobalIndexQueryServiceDescriptor> selected = selectGlobalIndexService(id);
            if (selected.isPresent()
                    && compareOwnerTokens(selected.get().ownerToken(), ownerToken) > 0) {
                // A newer attempt owns discovery. Remove only this attempt's file.
                fileIO.deleteQuietly(ownerServicePath(id, ownerToken));
                return;
            }

            GlobalIndexQueryServiceDescriptor owned;
            if (ownDescriptor.isPresent()) {
                owned = ownDescriptor.get();
            } else if (selected.isPresent() && selected.get().ownerToken().equals(ownerToken)) {
                // Compatibility path for a canonical descriptor written before owner-scoped files.
                owned = selected.get();
            } else {
                return;
            }

            GlobalIndexQueryServiceDescriptor tombstone =
                    new GlobalIndexQueryServiceDescriptor(
                            owned.protocolVersion(),
                            owned.tableUuid(),
                            owned.branch(),
                            owned.schemaId(),
                            owned.schemaFingerprint(),
                            owned.lookupFieldId(),
                            owned.valueFieldIds(),
                            Long.MIN_VALUE,
                            -1L,
                            null,
                            owned.hashVersion(),
                            owned.layout(),
                            ownerToken,
                            false,
                            "Query service publisher is closed.",
                            new GlobalIndexQueryServiceDescriptor.Endpoint[0]);
            fileIO.overwriteFileUtf8(
                    ownerServicePath(id, ownerToken), JsonSerdeUtil.toJson(tombstone));
            fileIO.deleteQuietly(servicePath(id));
            deleteOlderOwnerServices(id, ownerToken);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void deleteService(String id) {
        fileIO.deleteQuietly(servicePath(id));
    }

    private Path servicePath(String id) {
        return new Path(serviceDirectory() + "/" + SERVICE_PREFIX + id);
    }

    private Path serviceDirectory() {
        return new Path(tablePath + "/service");
    }

    private String ownerServicePrefix(String id) {
        return SERVICE_PREFIX + id + "-owner-";
    }

    private Path ownerServicePath(String id, String ownerToken) {
        String tokenHash =
                UUID.nameUUIDFromBytes(ownerToken.getBytes(StandardCharsets.UTF_8)).toString();
        return new Path(serviceDirectory() + "/" + ownerServicePrefix(id) + tokenHash);
    }

    private Optional<GlobalIndexQueryServiceDescriptor> readGlobalIndexDescriptor(Path path)
            throws IOException {
        return fileIO.readOverwrittenFileUtf8(path)
                .map(json -> JsonSerdeUtil.fromJson(json, GlobalIndexQueryServiceDescriptor.class));
    }

    private Optional<GlobalIndexQueryServiceDescriptor> selectGlobalIndexService(String id)
            throws IOException {
        Optional<GlobalIndexQueryServiceDescriptor> selected = Optional.empty();
        Path directory = serviceDirectory();
        if (fileIO.exists(directory)) {
            String prefix = ownerServicePrefix(id);
            for (FileStatus status : fileIO.listStatus(directory)) {
                if (status.isDir() || !status.getPath().getName().startsWith(prefix)) {
                    continue;
                }
                Optional<GlobalIndexQueryServiceDescriptor> descriptor =
                        readGlobalIndexDescriptor(status.getPath());
                if (descriptor.isPresent()
                        && (!selected.isPresent()
                                || compareOwnerTokens(
                                                descriptor.get().ownerToken(),
                                                selected.get().ownerToken())
                                        > 0)) {
                    selected = descriptor;
                }
            }
        }
        // Compatibility fallback for a descriptor written before owner-scoped files existed.
        return selected.isPresent() ? selected : readGlobalIndexDescriptor(servicePath(id));
    }

    private static int compareOwnerTokens(String left, String right) {
        long leftSequence = ownerSequence(left);
        long rightSequence = ownerSequence(right);
        if (leftSequence >= 0L || rightSequence >= 0L) {
            if (leftSequence < 0L) {
                return -1;
            }
            if (rightSequence < 0L) {
                return 1;
            }
            int sequenceComparison = Long.compare(leftSequence, rightSequence);
            if (sequenceComparison != 0) {
                return sequenceComparison;
            }
        }
        return left.compareTo(right);
    }

    private static long ownerSequence(String ownerToken) {
        int separator = ownerToken.indexOf('-');
        if (separator <= 0) {
            return -1L;
        }
        try {
            long sequence = Long.parseLong(ownerToken.substring(0, separator));
            return sequence < 0L ? -1L : sequence;
        } catch (NumberFormatException ignored) {
            return -1L;
        }
    }

    private void deleteOlderOwnerServices(String id, String ownerToken) throws IOException {
        Path directory = serviceDirectory();
        if (!fileIO.exists(directory)) {
            return;
        }
        String prefix = ownerServicePrefix(id);
        for (FileStatus status : fileIO.listStatus(directory)) {
            if (status.isDir() || !status.getPath().getName().startsWith(prefix)) {
                continue;
            }
            Optional<GlobalIndexQueryServiceDescriptor> descriptor =
                    readGlobalIndexDescriptor(status.getPath());
            if (!descriptor.isPresent()) {
                continue;
            }
            int comparison = compareOwnerTokens(descriptor.get().ownerToken(), ownerToken);
            if (comparison < 0) {
                fileIO.deleteQuietly(status.getPath());
            }
        }
    }
}
