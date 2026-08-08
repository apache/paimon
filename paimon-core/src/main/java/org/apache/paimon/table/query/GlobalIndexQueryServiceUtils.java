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

package org.apache.paimon.table.query;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.globalindex.DataEvolutionGlobalIndexCoverage;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexerFactory;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.GlobalIndexColumnUpdateAction.IGNORE;
import static org.apache.paimon.CoreOptions.GlobalIndexSearchMode.FAST;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.service.ServiceManager.globalIndexLookupService;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared validation, discovery, routing, and coverage rules for global-index query service. */
public final class GlobalIndexQueryServiceUtils {

    public static final long EMPTY_SNAPSHOT_ID = -1L;
    public static final int MAX_TOTAL_VALUE_BYTES = 64 * 1024 * 1024;

    private GlobalIndexQueryServiceUtils() {}

    public static QuerySpec querySpec(
            FileStoreTable table, String lookupField, List<String> valueFields) {
        checkArgument(
                table.schema().primaryKeys().isEmpty(),
                "Global-index query service only supports append tables without primary keys.");
        checkArgument(
                table.schema().partitionKeys().isEmpty(),
                "Global-index query service currently supports only unpartitioned tables so lookup-key uniqueness is global.");
        checkArgument(
                table.bucketMode() == BucketMode.BUCKET_UNAWARE,
                "Global-index query service requires bucket=-1, but table '%s' uses %s.",
                table.name(),
                table.bucketMode());
        CoreOptions options = table.coreOptions();
        CoreOptions persistedOptions = persistedCoreOptions(table);
        checkArgument(
                DEFAULT_MAIN_BRANCH.equals(options.branch()),
                "Global-index query service currently supports only the main branch because service discovery is table-scoped.");
        checkArgument(
                options.rowTrackingEnabled() && options.dataEvolutionEnabled(),
                "Global-index query service requires row-tracking.enabled=true and data-evolution.enabled=true.");
        checkArgument(
                options.globalIndexColumnUpdateAction() != IGNORE
                        && persistedOptions.globalIndexColumnUpdateAction() != IGNORE,
                "Global-index query service does not support global-index.column-update-action=IGNORE because it can return false misses.");
        checkArgument(
                !options.queryAuthEnabled() && !persistedOptions.queryAuthEnabled(),
                "Global-index query service does not support persisted or dynamic query-auth.enabled because its materialized state is shared by all clients.");
        checkArgument(
                !options.scanIgnoreCorruptFile()
                        && !options.scanIgnoreLostFile()
                        && !persistedOptions.scanIgnoreCorruptFile()
                        && !persistedOptions.scanIgnoreLostFile(),
                "Global-index query service requires persisted and dynamic scan.ignore-corrupt-files=false and scan.ignore-lost-files=false.");
        checkArgument(
                persistedOptions.consumerExpireTime() != null
                        && persistedOptions
                                        .consumerExpireTime()
                                        .compareTo(
                                                GlobalIndexQuerySnapshotLease.MIN_EXPIRATION_TIME)
                                >= 0,
                "Global-index query service requires a persisted consumer.expiration-time of at least %s so leases can be heartbeated safely and abandoned attempts can be cleaned up by writers.",
                GlobalIndexQuerySnapshotLease.MIN_EXPIRATION_TIME);
        checkArgument(
                !options.consumerChangelogOnly() && !persistedOptions.consumerChangelogOnly(),
                "Global-index query service requires persisted and dynamic consumer.changelog-only=false so its lease protects table snapshots.");
        checkArgument(
                valueFields != null && !valueFields.isEmpty(), "Value fields must not be empty.");

        RowType rowType = table.rowType();
        int lookupPosition = rowType.getFieldIndex(lookupField);
        checkArgument(lookupPosition >= 0, "Lookup field '%s' does not exist.", lookupField);
        int[] valuePositions = new int[valueFields.size()];
        int[] valueFieldIds = new int[valueFields.size()];
        for (int i = 0; i < valueFields.size(); i++) {
            String field = valueFields.get(i);
            int position = rowType.getFieldIndex(field);
            checkArgument(position >= 0, "Value field '%s' does not exist.", field);
            checkArgument(
                    !field.equals(lookupField),
                    "Lookup field '%s' must not also be a value field.",
                    lookupField);
            checkArgument(
                    !BlobType.isBlobFileField(rowType.getTypeAt(position))
                            || rowType.getTypeAt(position).getTypeRoot() == DataTypeRoot.BLOB,
                    "Global-index query service does not support nested BLOB value field '%s'.",
                    field);
            if (rowType.getTypeAt(position).getTypeRoot() == DataTypeRoot.BLOB) {
                checkArgument(
                        !options.blobInlineField().contains(field)
                                && !persistedOptions.blobInlineField().contains(field),
                        "Global-index query service only supports raw blob-file value field '%s'; inline descriptor and BlobView fields are not supported.",
                        field);
            }
            for (int j = 0; j < i; j++) {
                checkArgument(
                        !valueFields.get(j).equals(field), "Duplicate value field '%s'.", field);
            }
            valuePositions[i] = position;
            valueFieldIds[i] = rowType.getFields().get(position).id();
        }

        DataField lookupDataField = rowType.getFields().get(lookupPosition);
        checkArgument(
                !BlobType.isBlobFileField(lookupDataField.type()),
                "Global-index query service does not support BLOB lookup field '%s'.",
                lookupField);
        return new QuerySpec(
                lookupField,
                lookupPosition,
                lookupDataField.id(),
                valueFields,
                valuePositions,
                valueFieldIds,
                schemaFingerprint(table.schema().id(), rowType, lookupPosition, valuePositions),
                globalIndexLookupService(table.schema().id(), lookupDataField.id(), valueFieldIds));
    }

    private static String schemaFingerprint(
            long schemaId, RowType rowType, int lookupPosition, int[] valuePositions) {
        StringBuilder canonical = new StringBuilder().append(schemaId);
        DataField lookup = rowType.getFields().get(lookupPosition);
        canonical.append('|').append(lookup.id()).append(':').append(lookup.type());
        for (int position : valuePositions) {
            DataField value = rowType.getFields().get(position);
            canonical.append('|').append(value.id()).append(':').append(value.type());
        }
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format("%02x", value & 0xff));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    /** Stable key-only routing shared by the client, bootstrap shuffle, and server. */
    public static int route(BinaryRow key, int numShards) {
        checkArgument(numShards > 0, "Number of query shards must be positive.");
        return Math.floorMod(normalizeKey(key).hashCode(), numShards);
    }

    /** Lookup keys are value-only; row kind must not affect routing or state equality. */
    public static BinaryRow normalizeKey(BinaryRow key) {
        BinaryRow normalized = key.copy();
        normalized.setRowKind(RowKind.INSERT);
        return normalized;
    }

    /**
     * Read persisted schema options, excluding untrusted dynamic options from {@code table.copy}.
     */
    public static CoreOptions persistedCoreOptions(FileStoreTable table) {
        return CoreOptions.fromMap(table.schemaManager().schema(table.schema().id()).options());
    }

    /** Validate BTree coverage for one exact snapshot without opening every BTree reader. */
    public static SnapshotReadiness snapshotReadiness(
            FileStoreTable table, QuerySpec spec, @Nullable Snapshot snapshot) {
        if (snapshot == null) {
            return SnapshotReadiness.ready(EMPTY_SNAPSHOT_ID);
        }
        if (snapshot.schemaId() != table.schema().id()) {
            return SnapshotReadiness.notReady(
                    snapshot.id(),
                    String.format(
                            "Snapshot schema %s differs from query-service schema %s; restart the service.",
                            snapshot.schemaId(), table.schema().id()));
        }

        List<IndexFileMeta> indexFiles =
                table.store().newIndexFileHandler()
                        .scan(snapshot, entry -> isLookupBTree(entry, spec.lookupFieldId()))
                        .stream()
                        .map(IndexManifestEntry::indexFile)
                        .collect(Collectors.toList());
        DataEvolutionGlobalIndexCoverage coverage =
                new DataEvolutionGlobalIndexCoverage(table, snapshot, null, indexFiles, FAST);
        if (coverage.isFullyCovered(spec.lookupFieldId())) {
            // A snapshot with no live data files is complete without an index file.
            return SnapshotReadiness.ready(snapshot.id());
        }
        if (indexFiles.isEmpty()) {
            return SnapshotReadiness.notReady(
                    snapshot.id(),
                    String.format(
                            "No BTree global index is available for lookup field '%s'.",
                            spec.lookupField()));
        }
        return SnapshotReadiness.notReady(
                snapshot.id(),
                String.format(
                        "BTree global index for lookup field '%s' does not cover every live row range in snapshot %s.",
                        spec.lookupField(), snapshot.id()));
    }

    private static boolean isLookupBTree(IndexManifestEntry entry, int fieldId) {
        IndexFileMeta indexFile = entry.indexFile();
        GlobalIndexMeta meta = indexFile.globalIndexMeta();
        return meta != null
                && meta.indexFieldId() == fieldId
                && BTreeGlobalIndexerFactory.IDENTIFIER.equals(indexFile.indexType());
    }

    /** Immutable validated query schema. */
    public static final class QuerySpec {

        private final String lookupField;
        private final int lookupPosition;
        private final int lookupFieldId;
        private final List<String> valueFields;
        private final int[] valuePositions;
        private final int[] valueFieldIds;
        private final String schemaFingerprint;
        private final String serviceId;

        private QuerySpec(
                String lookupField,
                int lookupPosition,
                int lookupFieldId,
                List<String> valueFields,
                int[] valuePositions,
                int[] valueFieldIds,
                String schemaFingerprint,
                String serviceId) {
            this.lookupField = lookupField;
            this.lookupPosition = lookupPosition;
            this.lookupFieldId = lookupFieldId;
            this.valueFields = Collections.unmodifiableList(new ArrayList<>(valueFields));
            this.valuePositions = Arrays.copyOf(valuePositions, valuePositions.length);
            this.valueFieldIds = Arrays.copyOf(valueFieldIds, valueFieldIds.length);
            this.schemaFingerprint = schemaFingerprint;
            this.serviceId = serviceId;
        }

        public String lookupField() {
            return lookupField;
        }

        public int lookupPosition() {
            return lookupPosition;
        }

        public int lookupFieldId() {
            return lookupFieldId;
        }

        public List<String> valueFields() {
            return valueFields;
        }

        public int[] valuePositions() {
            return Arrays.copyOf(valuePositions, valuePositions.length);
        }

        public int[] valueFieldIds() {
            return Arrays.copyOf(valueFieldIds, valueFieldIds.length);
        }

        public String schemaFingerprint() {
            return schemaFingerprint;
        }

        public int[] bootstrapProjection() {
            int[] projection = new int[valuePositions.length + 1];
            projection[0] = lookupPosition;
            System.arraycopy(valuePositions, 0, projection, 1, valuePositions.length);
            return projection;
        }

        public String serviceId() {
            return serviceId;
        }
    }

    /** Snapshot readiness is deliberately different from an empty lookup result. */
    public static final class SnapshotReadiness {

        private final long snapshotId;
        private final boolean ready;
        private final String reason;

        private SnapshotReadiness(long snapshotId, boolean ready, String reason) {
            this.snapshotId = snapshotId;
            this.ready = ready;
            this.reason = reason;
        }

        public static SnapshotReadiness ready(long snapshotId) {
            return new SnapshotReadiness(snapshotId, true, "");
        }

        public static SnapshotReadiness notReady(long snapshotId, String reason) {
            return new SnapshotReadiness(snapshotId, false, reason);
        }

        public long snapshotId() {
            return snapshotId;
        }

        public boolean ready() {
            return ready;
        }

        public String reason() {
            return reason;
        }
    }
}
