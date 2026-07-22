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

package org.apache.paimon.clone;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.IndexManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.cache.Cache;
import org.apache.paimon.shade.guava30.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Validates that each cloned history root resolves to the same canonical metadata content. */
class FullHistoryRootValidator {

    private static final long MAX_CACHED_METADATA_DIGESTS = 10_000;

    static void validate(
            FileStoreTable source, FileStoreTable target, PathMapping pathMapping, String branch)
            throws IOException {
        RootDigestContext sourceContext = new RootDigestContext(source, pathMapping, true);
        RootDigestContext targetContext = new RootDigestContext(target, pathMapping, false);

        Map<Long, Snapshot> targetSnapshots =
                target.snapshotManager().safelyGetAllSnapshots().stream()
                        .collect(Collectors.toMap(Snapshot::id, snapshot -> snapshot));
        for (Snapshot sourceSnapshot : source.snapshotManager().safelyGetAllSnapshots()) {
            Snapshot targetSnapshot = targetSnapshots.get(sourceSnapshot.id());
            checkState(
                    Arrays.equals(
                            sourceContext.snapshotDigest(sourceSnapshot),
                            targetContext.snapshotDigest(targetSnapshot)),
                    "Target snapshot %s in branch %s does not match source semantics.",
                    sourceSnapshot.id(),
                    branch);
        }

        Map<String, Tag> targetTags = new HashMap<>();
        for (Pair<Tag, String> tagAndName : target.tagManager().tagObjects()) {
            targetTags.put(tagAndName.getRight(), tagAndName.getLeft());
        }
        for (Pair<Tag, String> tagAndName : source.tagManager().tagObjects()) {
            String tagName = tagAndName.getRight();
            checkState(
                    Arrays.equals(
                            sourceContext.snapshotDigest(tagAndName.getLeft()),
                            targetContext.snapshotDigest(targetTags.get(tagName))),
                    "Target tag %s in branch %s does not match source semantics.",
                    tagName,
                    branch);
        }

        Map<Long, Changelog> targetChangelogs =
                target.changelogManager().safelyGetAllChangelogs().stream()
                        .collect(Collectors.toMap(Changelog::id, changelog -> changelog));
        for (Changelog sourceChangelog : source.changelogManager().safelyGetAllChangelogs()) {
            Changelog targetChangelog = targetChangelogs.get(sourceChangelog.id());
            checkState(
                    Arrays.equals(
                            sourceContext.changelogDigest(sourceChangelog),
                            targetContext.changelogDigest(targetChangelog)),
                    "Target changelog %s in branch %s does not match source semantics.",
                    sourceChangelog.id(),
                    branch);
        }
    }

    private static class RootDigestContext {

        private final PathMapping pathMapping;
        private final boolean rewritePaths;
        private final RowType partitionType;
        private final ManifestList manifestList;
        private final ManifestFile manifestFile;
        private final IndexFileHandler indexFileHandler;
        private final ManifestEntrySerializer manifestEntrySerializer =
                new ManifestEntrySerializer();
        private final IndexManifestEntrySerializer indexEntrySerializer =
                new IndexManifestEntrySerializer();
        private final MessageDigest entryHasher = newSha256();
        private final Cache<String, ManifestDigest> manifestDigests = newDigestCache();
        private final Cache<String, ManifestListDigest> manifestListDigests = newDigestCache();
        private final Cache<String, EntryMultisetDigest> indexManifestDigests = newDigestCache();

        private RootDigestContext(
                FileStoreTable table, PathMapping pathMapping, boolean rewritePaths) {
            this.pathMapping = pathMapping;
            this.rewritePaths = rewritePaths;
            this.partitionType = table.schema().logicalPartitionType();
            this.manifestList = table.store().manifestListFactory().create();
            this.manifestFile = table.store().manifestFileFactory().create();
            this.indexFileHandler = table.store().newIndexFileHandler();
        }

        private byte[] snapshotDigest(Snapshot snapshot) throws IOException {
            return rootDigest(snapshot, false);
        }

        private byte[] changelogDigest(Changelog changelog) throws IOException {
            return rootDigest(changelog, true);
        }

        private byte[] rootDigest(Snapshot snapshot, boolean longLivedChangelog)
                throws IOException {
            checkState(snapshot != null, "Target history root does not exist.");
            CanonicalDigest digest = new CanonicalDigest();
            digest.addInt(snapshot.version());
            digest.addLong(snapshot.id());
            digest.addLong(snapshot.schemaId());

            boolean dedicatedChangelog =
                    longLivedChangelog && snapshot.changelogManifestList() != null;
            if (dedicatedChangelog) {
                digest.addString(snapshot.baseManifestList());
                digest.addNullableLong(snapshot.baseManifestListSize());
                digest.addString(snapshot.deltaManifestList());
                digest.addNullableLong(snapshot.deltaManifestListSize());
            } else {
                digest.addBytes(
                        manifestListDigest(
                                        snapshot.baseManifestList(),
                                        snapshot.baseManifestListSize())
                                .toBytes());
                digest.addBytes(
                        manifestListDigest(
                                        snapshot.deltaManifestList(),
                                        snapshot.deltaManifestListSize())
                                .toBytes());
            }

            digest.addNullableBytes(
                    snapshot.changelogManifestList() == null
                            ? null
                            : manifestListDigest(
                                            snapshot.changelogManifestList(),
                                            snapshot.changelogManifestListSize())
                                    .toBytes());
            if (longLivedChangelog) {
                // Index and statistics metadata are retained verbatim by changelog expiration.
                digest.addString(snapshot.indexManifest());
            } else {
                digest.addNullableBytes(
                        snapshot.indexManifest() == null
                                ? null
                                : indexManifestDigest(snapshot.indexManifest()).toBytes());
            }

            digest.addString(snapshot.commitUser());
            digest.addLong(snapshot.commitIdentifier());
            digest.addString(snapshot.commitKind().name());
            digest.addLong(snapshot.timeMillis());
            digest.addLong(snapshot.totalRecordCount());
            digest.addLong(snapshot.deltaRecordCount());
            digest.addNullableLong(snapshot.changelogRecordCount());
            digest.addNullableLong(snapshot.watermark());
            digest.addString(snapshot.statistics());
            digest.addMap(snapshot.properties());
            digest.addNullableLong(snapshot.nextRowId());
            digest.addString(snapshot.operation() == null ? null : snapshot.operation().name());
            return digest.finish();
        }

        private EntryMultisetDigest manifestListDigest(
                String fileName, @Nullable Long declaredFileSize) throws IOException {
            ManifestListDigest cached = manifestListDigests.getIfPresent(fileName);
            if (cached != null) {
                validateFileSize("manifest list", fileName, declaredFileSize, cached.fileSize);
                return cached.entries;
            }
            Long actualFileSize = rewritePaths ? null : manifestList.fileSize(fileName);
            validateFileSize("manifest list", fileName, declaredFileSize, actualFileSize);
            // Rewritten paths can change serialized sizes and roll one source manifest into
            // multiple target manifests. Compare entries at root level and validate each target
            // manifest's pruning metadata against its own entries in manifestDigest.
            EntryMultisetDigest digest = new EntryMultisetDigest();
            for (ManifestFileMeta meta :
                    manifestList.readWithIOException(fileName, declaredFileSize)) {
                digest.merge(manifestDigest(meta));
            }
            manifestListDigests.put(fileName, new ManifestListDigest(digest, actualFileSize));
            return digest;
        }

        private void validateFileSize(
                String fileType,
                String fileName,
                @Nullable Long declaredFileSize,
                @Nullable Long actualFileSize) {
            if (!rewritePaths && declaredFileSize != null) {
                checkState(
                        actualFileSize != null && actualFileSize.longValue() == declaredFileSize,
                        "Target %s %s has size %s but metadata records %s.",
                        fileType,
                        fileName,
                        actualFileSize,
                        declaredFileSize);
            }
        }

        private EntryMultisetDigest manifestDigest(ManifestFileMeta meta) throws IOException {
            ManifestDigest cached = manifestDigests.getIfPresent(meta.fileName());
            if (cached != null) {
                validateManifest(meta, cached);
                return cached.entries;
            }
            Long actualFileSize = rewritePaths ? null : manifestFile.fileSize(meta.fileName());
            EntryMultisetDigest digest = new EntryMultisetDigest();
            List<ManifestEntry> entries =
                    manifestFile.readWithIOException(meta.fileName(), meta.fileSize());
            for (ManifestEntry entry : entries) {
                digest.add(entryHasher, manifestEntrySerializer.serializeToBytes(canonical(entry)));
            }
            ManifestMetadata metadata = ManifestMetadata.from(entries, partitionType);
            ManifestDigest manifestDigest = new ManifestDigest(digest, metadata, actualFileSize);
            validateManifest(meta, manifestDigest);
            manifestDigests.put(meta.fileName(), manifestDigest);
            return digest;
        }

        private void validateManifest(ManifestFileMeta manifest, ManifestDigest expected) {
            validateFileSize(
                    "manifest file", manifest.fileName(), manifest.fileSize(), expected.fileSize);
            if (!rewritePaths) {
                checkState(
                        expected.metadata.matches(manifest),
                        "Target manifest metadata %s does not match its entries.",
                        manifest.fileName());
            }
        }

        private ManifestEntry canonical(ManifestEntry entry) {
            if (!rewritePaths || !entry.file().externalPath().isPresent()) {
                return entry;
            }
            DataFileMeta sourceFile = entry.file();
            DataFileMeta targetFile =
                    sourceFile.newExternalPath(
                            pathMapping.rewriteRequired(sourceFile.externalPath().get()));
            return ManifestEntry.create(
                    entry.kind(),
                    entry.partition(),
                    entry.bucket(),
                    entry.totalBuckets(),
                    targetFile);
        }

        private EntryMultisetDigest indexManifestDigest(String fileName) throws IOException {
            EntryMultisetDigest cached = indexManifestDigests.getIfPresent(fileName);
            if (cached != null) {
                return cached;
            }
            EntryMultisetDigest digest = new EntryMultisetDigest();
            for (IndexManifestEntry entry :
                    indexFileHandler.readManifestWithIOException(fileName)) {
                digest.add(entryHasher, indexEntrySerializer.serializeToBytes(canonical(entry)));
            }
            indexManifestDigests.put(fileName, digest);
            return digest;
        }

        private IndexManifestEntry canonical(IndexManifestEntry entry) {
            IndexFileMeta sourceFile = entry.indexFile();
            if (!rewritePaths || sourceFile.externalPath() == null) {
                return entry;
            }
            IndexFileMeta targetFile =
                    new IndexFileMeta(
                            sourceFile.indexType(),
                            sourceFile.fileName(),
                            sourceFile.fileSize(),
                            sourceFile.rowCount(),
                            sourceFile.dvRanges(),
                            pathMapping.rewriteRequired(sourceFile.externalPath()),
                            sourceFile.globalIndexMeta());
            return new IndexManifestEntry(
                    entry.kind(), entry.partition(), entry.bucket(), targetFile);
        }
    }

    private static class ManifestDigest {

        private final EntryMultisetDigest entries;
        private final ManifestMetadata metadata;
        private final @Nullable Long fileSize;

        private ManifestDigest(
                EntryMultisetDigest entries, ManifestMetadata metadata, @Nullable Long fileSize) {
            this.entries = entries;
            this.metadata = metadata;
            this.fileSize = fileSize;
        }
    }

    private static class ManifestListDigest {

        private final EntryMultisetDigest entries;
        private final @Nullable Long fileSize;

        private ManifestListDigest(EntryMultisetDigest entries, @Nullable Long fileSize) {
            this.entries = entries;
            this.fileSize = fileSize;
        }
    }

    private static class ManifestMetadata {

        private final long numAddedFiles;
        private final long numDeletedFiles;
        private final SimpleStats partitionStats;
        private final long schemaId;
        private final Integer minBucket;
        private final Integer maxBucket;
        private final Integer minLevel;
        private final Integer maxLevel;
        private final Long minRowId;
        private final Long maxRowId;

        private ManifestMetadata(
                long numAddedFiles,
                long numDeletedFiles,
                SimpleStats partitionStats,
                long schemaId,
                Integer minBucket,
                Integer maxBucket,
                Integer minLevel,
                Integer maxLevel,
                Long minRowId,
                Long maxRowId) {
            this.numAddedFiles = numAddedFiles;
            this.numDeletedFiles = numDeletedFiles;
            this.partitionStats = partitionStats;
            this.schemaId = schemaId;
            this.minBucket = minBucket;
            this.maxBucket = maxBucket;
            this.minLevel = minLevel;
            this.maxLevel = maxLevel;
            this.minRowId = minRowId;
            this.maxRowId = maxRowId;
        }

        private static ManifestMetadata from(List<ManifestEntry> entries, RowType partitionType) {
            long numAddedFiles = 0;
            long numDeletedFiles = 0;
            long schemaId = Long.MIN_VALUE;
            int minBucket = Integer.MAX_VALUE;
            int maxBucket = Integer.MIN_VALUE;
            int minLevel = Integer.MAX_VALUE;
            int maxLevel = Integer.MIN_VALUE;
            long minRowId = Long.MAX_VALUE;
            long maxRowId = Long.MIN_VALUE;
            boolean hasRowIds = true;
            SimpleStatsCollector partitionStatsCollector = new SimpleStatsCollector(partitionType);

            for (ManifestEntry entry : entries) {
                switch (entry.kind()) {
                    case ADD:
                        numAddedFiles++;
                        break;
                    case DELETE:
                        numDeletedFiles++;
                        break;
                    default:
                        throw new UnsupportedOperationException(
                                "Unknown entry kind: " + entry.kind());
                }
                schemaId = Math.max(schemaId, entry.file().schemaId());
                minBucket = Math.min(minBucket, entry.bucket());
                maxBucket = Math.max(maxBucket, entry.bucket());
                minLevel = Math.min(minLevel, entry.level());
                maxLevel = Math.max(maxLevel, entry.level());
                Long firstRowId = entry.file().firstRowId();
                if (firstRowId == null) {
                    hasRowIds = false;
                } else if (hasRowIds) {
                    minRowId = Math.min(minRowId, firstRowId);
                    maxRowId = Math.max(maxRowId, firstRowId + entry.file().rowCount() - 1);
                }
                partitionStatsCollector.collect(entry.partition());
            }

            checkState(!entries.isEmpty(), "Manifest file must contain at least one entry.");
            SimpleStats partitionStats =
                    new SimpleStatsConverter(partitionType)
                            .toBinaryAllMode(partitionStatsCollector.extract());
            return new ManifestMetadata(
                    numAddedFiles,
                    numDeletedFiles,
                    partitionStats,
                    schemaId,
                    minBucket,
                    maxBucket,
                    minLevel,
                    maxLevel,
                    hasRowIds ? minRowId : null,
                    hasRowIds ? maxRowId : null);
        }

        private boolean matches(ManifestFileMeta meta) {
            return numAddedFiles == meta.numAddedFiles()
                    && numDeletedFiles == meta.numDeletedFiles()
                    && Objects.equals(partitionStats, meta.partitionStats())
                    && schemaId == meta.schemaId()
                    && Objects.equals(minBucket, meta.minBucket())
                    && Objects.equals(maxBucket, meta.maxBucket())
                    && Objects.equals(minLevel, meta.minLevel())
                    && Objects.equals(maxLevel, meta.maxLevel())
                    && Objects.equals(minRowId, meta.minRowId())
                    && Objects.equals(maxRowId, meta.maxRowId());
        }
    }

    private static class EntryMultisetDigest {

        private final byte[] sum = new byte[32];
        private long count;

        private void add(MessageDigest hasher, byte[] value) {
            hasher.reset();
            addHash(hasher.digest(value));
            count++;
        }

        private void merge(EntryMultisetDigest other) {
            addHash(other.sum);
            count += other.count;
        }

        private void addHash(byte[] hash) {
            int carry = 0;
            for (int i = sum.length - 1; i >= 0; i--) {
                int value = (sum[i] & 0xff) + (hash[i] & 0xff) + carry;
                sum[i] = (byte) value;
                carry = value >>> 8;
            }
        }

        private byte[] toBytes() {
            byte[] result = new byte[Long.BYTES + sum.length];
            writeLong(result, 0, count);
            System.arraycopy(sum, 0, result, Long.BYTES, sum.length);
            return result;
        }
    }

    private static class CanonicalDigest {

        private final MessageDigest digest = newSha256();

        private void addInt(int value) {
            addLong(value);
        }

        private void addLong(long value) {
            byte[] bytes = new byte[Long.BYTES];
            writeLong(bytes, 0, value);
            digest.update(bytes);
        }

        private void addNullableLong(@Nullable Long value) {
            addLong(value == null ? -1 : 1);
            if (value != null) {
                addLong(value);
            }
        }

        private void addString(@Nullable String value) {
            addNullableBytes(value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        }

        private void addBytes(byte[] value) {
            addNullableBytes(value);
        }

        private void addNullableBytes(@Nullable byte[] value) {
            addLong(value == null ? -1 : value.length);
            if (value != null) {
                digest.update(value);
            }
        }

        private void addMap(@Nullable Map<String, String> values) {
            if (values == null) {
                addLong(-1);
                return;
            }
            Map<String, String> sorted = new TreeMap<>(values);
            addLong(sorted.size());
            for (Map.Entry<String, String> entry : sorted.entrySet()) {
                addString(entry.getKey());
                addString(entry.getValue());
            }
        }

        private byte[] finish() {
            return digest.digest();
        }
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static <T> Cache<String, T> newDigestCache() {
        return CacheBuilder.newBuilder().maximumSize(MAX_CACHED_METADATA_DIGESTS).build();
    }

    private static void writeLong(byte[] target, int offset, long value) {
        for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
            target[offset++] = (byte) (value >>> shift);
        }
    }

    private FullHistoryRootValidator() {}
}
