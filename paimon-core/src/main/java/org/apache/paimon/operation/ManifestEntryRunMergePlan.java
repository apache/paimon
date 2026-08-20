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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.avro.AvroRawBlock;
import org.apache.paimon.manifest.CollectedDeletes;
import org.apache.paimon.manifest.CompactFileIdentifierSet;
import org.apache.paimon.manifest.FileEntry.ReusableIdentifier;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestAvroReader;
import org.apache.paimon.manifest.ManifestAvroReader.RawBlock;
import org.apache.paimon.manifest.ManifestAvroReader.RowIterator;
import org.apache.paimon.manifest.ManifestAvroWriter;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedBlockMeta;
import org.apache.paimon.manifest.ManifestAvroWriter.EncodedEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ProjectedManifestEntry;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkState;

final class ManifestEntryRunMergePlan {

    final List<Source.Spec> sources;
    final ManifestEntryRunMerge.SortPartitionDictionary partitions;
    final CollectedDeletes deletes;
    final boolean minor;

    ManifestEntryRunMergePlan(
            List<Source.Spec> sources,
            ManifestEntryRunMerge.SortPartitionDictionary partitions,
            CollectedDeletes deletes,
            boolean minor) {
        this.sources = sources;
        this.partitions = partitions;
        this.deletes = deletes;
        this.minor = minor;
    }

    List<ManifestFileMeta> mergeToManifest(
            ManifestFile manifestFile, List<ManifestFileMeta> newFilesForAbort) throws Exception {
        List<Cursor> cursors = new ArrayList<>(sources.size());
        List<ManifestFileMeta> files = Collections.emptyList();
        Throwable primaryFailure = null;
        try {
            for (Source.Spec source : sources) {
                Cursor cursor = source.open(manifestFile, deletes, minor, partitions);
                cursors.add(cursor);
                cursor.advance();
            }
            SelectionTree selectionTree = new SelectionTree(cursors);
            if (selectionTree.winner() >= 0) {
                files = writeSelected(selectionTree, manifestFile);
                newFilesForAbort.addAll(files);
            }
        } catch (Throwable failure) {
            primaryFailure = failure;
        } finally {
            try {
                closeCursors(cursors);
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
        }
        if (primaryFailure != null) {
            ExceptionUtils.rethrowException(primaryFailure);
        }
        return files;
    }

    Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> mergeMinorToManifest(
            ManifestFile manifestFile, List<ManifestFileMeta> newFilesForAbort) throws Exception {
        List<Cursor> cursors = new ArrayList<>(sources.size());
        Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> files =
                Pair.of(Collections.emptyList(), Collections.emptyList());
        Throwable primaryFailure = null;
        try {
            for (Source.Spec source : sources) {
                Cursor cursor = source.open(manifestFile, deletes, minor, partitions);
                cursors.add(cursor);
                cursor.advance();
            }
            SelectionTree selectionTree = new SelectionTree(cursors);
            if (selectionTree.winner() >= 0) {
                files = writeMinorSelected(selectionTree, manifestFile, deletes);
                newFilesForAbort.addAll(files.getLeft());
                newFilesForAbort.addAll(files.getRight());
            }
        } catch (Throwable failure) {
            primaryFailure = failure;
        } finally {
            try {
                closeCursors(cursors);
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
        }
        if (primaryFailure != null) {
            ExceptionUtils.rethrowException(primaryFailure);
        }
        return files;
    }

    static List<ManifestFileMeta> writeSelected(
            SelectionTree selectionTree, ManifestFile manifestFile) throws Exception {
        ManifestAvroWriter writer = manifestFile.createAvroWriter();
        List<ManifestFileMeta> files = Collections.emptyList();
        Throwable primaryFailure = null;
        try {
            int winner;
            while ((winner = selectionTree.winner()) >= 0) {
                Cursor cursor = selectionTree.cursor(winner);
                if (cursor.hasCopyableBlock()
                        && selectionTree.blockPrecedesOthers(winner, cursor.blockLastKey())) {
                    writer.writeEncodedBlock(cursor.encodedBlock(), cursor.blockMetadata());
                    selectionTree.update(winner, cursor.advanceAfterBlock());
                    continue;
                }
                cursor.materializeCurrent();
                writeCurrent(writer, cursor);
                selectionTree.update(winner, cursor.advance());
            }
            writer.close();
            files = writer.result();
        } catch (Throwable failure) {
            primaryFailure = failure;
        } finally {
            if (primaryFailure != null) {
                writer.abort(primaryFailure);
            }
        }
        if (primaryFailure != null) {
            ExceptionUtils.rethrowException(primaryFailure);
        }
        return files;
    }

    private static Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> writeMinorSelected(
            SelectionTree selectionTree, ManifestFile manifestFile, CollectedDeletes deletes)
            throws Exception {
        ManifestAvroWriter addWriter = manifestFile.createAvroWriter();
        ManifestAvroWriter deleteWriter = manifestFile.createAvroWriter();
        CompactFileIdentifierSet matchedEntries = new CompactFileIdentifierSet();
        CompactFileIdentifierSet emittedDeletes = new CompactFileIdentifierSet();
        Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> files =
                Pair.of(Collections.emptyList(), Collections.emptyList());
        Throwable primaryFailure = null;
        try {
            int winner;
            while ((winner = selectionTree.winner()) >= 0) {
                Cursor cursor = selectionTree.cursor(winner);
                if (cursor.hasCopyableBlock()
                        && selectionTree.blockPrecedesOthers(winner, cursor.blockLastKey())) {
                    addWriter.writeEncodedBlock(cursor.encodedBlock(), cursor.blockMetadata());
                    selectionTree.update(winner, cursor.advanceAfterBlock());
                    continue;
                }

                cursor.materializeCurrent();
                if (cursor.key().kind == FileKind.ADD.toByteValue()) {
                    ReusableIdentifier identifier = cursor.identifier();
                    if (deletes.isDeleted(cursor.current(), identifier)) {
                        matchedEntries.add(identifier);
                    } else {
                        writeCurrent(addWriter, cursor);
                    }
                } else {
                    ReusableIdentifier identifier = cursor.identifier();
                    if (!matchedEntries.contains(identifier)
                            && !emittedDeletes.contains(identifier)) {
                        emittedDeletes.add(identifier);
                        writeCurrent(deleteWriter, cursor);
                    }
                }
                selectionTree.update(winner, cursor.advance());
            }
            addWriter.close();
            deleteWriter.close();
            files = Pair.of(addWriter.result(), deleteWriter.result());
        } catch (Throwable failure) {
            primaryFailure = failure;
        } finally {
            try {
                matchedEntries.release();
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
            try {
                emittedDeletes.release();
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
            if (primaryFailure != null) {
                addWriter.abort(primaryFailure);
                deleteWriter.abort(primaryFailure);
            }
        }
        if (primaryFailure != null) {
            ExceptionUtils.rethrowException(primaryFailure);
        }
        return files;
    }

    private static void writeCurrent(ManifestAvroWriter writer, Cursor cursor) throws Exception {
        InternalRow decodedRow = cursor.decodedRow();
        if (decodedRow != null) {
            writer.writeRow(decodedRow, cursor.metadata());
            return;
        }
        ByteBuffer encodedRecord = cursor.encodedRecord();
        if (encodedRecord == null) {
            writer.write(cursor.current());
        } else {
            writer.writeEncoded(encodedRecord, cursor.metadata());
        }
    }

    static void closeCursors(List<Cursor> cursors) throws Exception {
        Throwable primaryFailure = null;
        for (Cursor cursor : cursors) {
            try {
                cursor.close();
            } catch (Throwable cleanupFailure) {
                primaryFailure = ExceptionUtils.firstOrSuppressed(cleanupFailure, primaryFailure);
            }
        }
        if (primaryFailure != null) {
            ExceptionUtils.rethrowException(primaryFailure);
        }
    }

    /** Describes the manifest inputs which become cursors when this plan starts executing. */
    static final class Source {

        private Source() {}

        interface Spec {

            Cursor open(
                    ManifestFile manifestFile,
                    CollectedDeletes deletes,
                    boolean minor,
                    ManifestEntryRunMerge.SortPartitionDictionary partitions)
                    throws Exception;
        }

        static final class ManifestRunSpec implements Spec {

            final ManifestFileMeta meta;
            final long start;
            final long end;
            final List<ManifestEntryRunMerge.Discovery.BlockInfo> blocks;

            ManifestRunSpec(
                    ManifestFileMeta meta,
                    long start,
                    long end,
                    List<ManifestEntryRunMerge.Discovery.BlockInfo> blocks) {
                this.meta = meta;
                this.start = start;
                this.end = end;
                this.blocks = blocks;
            }

            long prefixBlockCount() {
                long lastBlockOrdinal = -1;
                for (ManifestEntryRunMerge.Discovery.BlockInfo block : blocks) {
                    if (block.start >= end) {
                        break;
                    }
                    if (block.end > start) {
                        lastBlockOrdinal = block.ordinal;
                    }
                }
                checkState(lastBlockOrdinal >= 0, "Manifest run does not contain an Avro block.");
                return lastBlockOrdinal + 1;
            }

            @Override
            public Cursor open(
                    ManifestFile manifestFile,
                    CollectedDeletes deletes,
                    boolean minor,
                    ManifestEntryRunMerge.SortPartitionDictionary partitions)
                    throws Exception {
                return new PrimitiveManifestRunCursor(
                        manifestFile, meta, start, end, blocks, deletes, minor, partitions);
            }
        }

        static final class FragmentedManifestSpec implements Spec {

            final ManifestFileMeta meta;

            FragmentedManifestSpec(ManifestFileMeta meta) {
                this.meta = meta;
            }

            @Override
            public Cursor open(
                    ManifestFile manifestFile,
                    CollectedDeletes deletes,
                    boolean minor,
                    ManifestEntryRunMerge.SortPartitionDictionary partitions)
                    throws Exception {
                return new InMemoryManifestCursor(manifestFile, meta, deletes, minor, partitions);
            }
        }
    }

    interface Cursor extends AutoCloseable {

        boolean advance() throws Exception;

        boolean hasCurrent();

        @Nullable
        ProjectedManifestEntry current();

        @Nullable
        EncodedEntry metadata();

        ManifestEntryRunMerge.SortKey key();

        @Nullable
        ByteBuffer encodedRecord();

        default @Nullable InternalRow decodedRow() {
            return null;
        }

        ReusableIdentifier identifier();

        default boolean hasCopyableBlock() {
            return false;
        }

        default ManifestEntryRunMerge.SortKey blockLastKey() {
            throw new UnsupportedOperationException();
        }

        default AvroRawBlock encodedBlock() {
            throw new UnsupportedOperationException();
        }

        default EncodedBlockMeta blockMetadata() {
            throw new UnsupportedOperationException();
        }

        default boolean advanceAfterBlock() throws Exception {
            throw new UnsupportedOperationException();
        }

        default void materializeCurrent() throws Exception {}

        @Override
        void close() throws Exception;
    }

    static final class PrimitiveManifestRunCursor implements Cursor {

        final ManifestAvroReader reader;
        final boolean encodedRecordsCompatible;
        final CollectedDeletes deletes;
        final boolean minor;
        final ManifestEntryRunMerge.SortPartitionDictionary partitions;
        final ManifestEntryRunMerge.SortKey key = new ManifestEntryRunMerge.SortKey();
        final EncodedEntry metadata = new EncodedEntry();
        final ProjectedManifestEntry projectedEntry =
                ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.createEntry();
        final ProjectedManifestEntry fullEntry =
                ProjectedManifestEntry.fullProjection().createEntry();
        final ReusableIdentifier identifier = new ReusableIdentifier();
        final List<ManifestEntryRunMerge.Discovery.BlockInfo> blocks;
        final long runStart;
        final long runEnd;
        int blockIndex;
        long nextReaderBlockOrdinal;
        long decodedRemaining;
        boolean rawBlock;
        boolean current;
        @Nullable RawBlock currentRawBlock;
        @Nullable RowIterator currentRows;
        @Nullable GenericRow currentSourceRow;
        @Nullable ProjectedManifestEntry currentEntry;
        @Nullable ManifestEntryRunMerge.Discovery.BlockInfo currentBlock;
        boolean closed;

        PrimitiveManifestRunCursor(
                ManifestFile manifestFile,
                ManifestFileMeta meta,
                long start,
                long end,
                List<ManifestEntryRunMerge.Discovery.BlockInfo> blocks,
                CollectedDeletes deletes,
                boolean minor,
                ManifestEntryRunMerge.SortPartitionDictionary partitions)
                throws Exception {
            this.reader = manifestFile.scanAvroBlocks(meta.fileName(), meta.fileSize());
            this.encodedRecordsCompatible = reader.rawBlockCopySupported();
            this.deletes = deletes;
            this.minor = minor;
            this.partitions = partitions;
            this.blocks = blocks;
            this.runStart = start;
            this.runEnd = end;
            try {
                while (blockIndex < blocks.size() && blocks.get(blockIndex).end <= start) {
                    blockIndex++;
                }
                checkState(
                        blockIndex < blocks.size(),
                        "Manifest run starts after the end of the file.");
            } catch (Exception e) {
                try {
                    reader.close();
                } catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        @Override
        public boolean advance() throws Exception {
            current = false;
            while (true) {
                if (decodedRemaining == 0) {
                    if (!prepareNextBlock()) {
                        key.clear();
                        close();
                        return false;
                    }
                    if (rawBlock) {
                        return true;
                    }
                }
                checkState(
                        currentRows != null && currentRows.hasNext(),
                        "Manifest block ends before its discovered boundary.");
                currentSourceRow = currentRows.next();
                currentEntry =
                        encodedRecordsCompatible
                                ? projectedEntry.replace(currentSourceRow)
                                : fullEntry.replace(currentSourceRow);
                decodedRemaining--;
                key.replace(currentEntry, partitions);
                if (minor || deletes.copyable(currentEntry, identifier, false)) {
                    current = true;
                    metadata.replace(
                            key.kind,
                            partitions.partition(key.partitionId),
                            currentEntry.bucket(),
                            currentEntry.file().level(),
                            currentEntry.file().schemaId(),
                            key.firstRowId,
                            currentEntry.file().rowCount());
                    return true;
                }
            }
        }

        boolean prepareNextBlock() throws Exception {
            rawBlock = false;
            current = false;
            currentRows = null;
            currentSourceRow = null;
            currentEntry = null;
            while (blockIndex < blocks.size()) {
                ManifestEntryRunMerge.Discovery.BlockInfo info = blocks.get(blockIndex);
                if (info.start >= runEnd) {
                    return false;
                }
                while (nextReaderBlockOrdinal < info.ordinal) {
                    checkState(reader.hasNext(), "Manifest block ordinal is missing.");
                    reader.next();
                    nextReaderBlockOrdinal++;
                }
                checkState(reader.hasNext(), "Manifest run ends after the end of the file.");
                currentRawBlock = reader.next();
                nextReaderBlockOrdinal++;
                currentBlock = info;
                if (info.copyable(runStart, runEnd)) {
                    rawBlock = true;
                    key.copyFrom(info.firstKey);
                    return true;
                }

                long overlapStart = Math.max(runStart, info.start);
                long overlapEnd = Math.min(runEnd, info.end);
                long prefix = overlapStart - info.start;
                currentRows =
                        currentRawBlock.toRows(
                                encodedRecordsCompatible
                                        ? ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION
                                                .projectedType()
                                        : ManifestEntry.MANIFEST_ROW_TYPE);
                for (long i = 0; i < prefix; i++) {
                    checkState(
                            currentRows.hasNext(),
                            "Manifest run starts after the end of its block.");
                    currentRows.next();
                }
                decodedRemaining = overlapEnd - overlapStart;
                blockIndex++;
                if (decodedRemaining > 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasCurrent() {
            return current || rawBlock;
        }

        @Override
        public ProjectedManifestEntry current() {
            return current ? currentEntry : null;
        }

        @Override
        public EncodedEntry metadata() {
            return metadata;
        }

        @Override
        public ManifestEntryRunMerge.SortKey key() {
            return key;
        }

        @Override
        public ByteBuffer encodedRecord() {
            return current && encodedRecordsCompatible ? currentRows.encodedRecord() : null;
        }

        @Override
        public InternalRow decodedRow() {
            return current && !encodedRecordsCompatible ? currentSourceRow : null;
        }

        @Override
        public ReusableIdentifier identifier() {
            checkState(current, "Manifest entry has not been materialized.");
            return identifier.replaceWithPartition(currentEntry);
        }

        @Override
        public boolean hasCopyableBlock() {
            return rawBlock;
        }

        @Override
        public ManifestEntryRunMerge.SortKey blockLastKey() {
            return currentBlock.lastKey;
        }

        @Override
        public AvroRawBlock encodedBlock() {
            return currentRawBlock.encodedBlock();
        }

        @Override
        public EncodedBlockMeta blockMetadata() {
            return currentBlock.metadata;
        }

        @Override
        public boolean advanceAfterBlock() throws Exception {
            checkState(rawBlock, "There is no raw block to advance.");
            rawBlock = false;
            currentRawBlock = null;
            blockIndex++;
            return advance();
        }

        @Override
        public void materializeCurrent() throws Exception {
            if (!rawBlock) {
                return;
            }
            rawBlock = false;
            decodedRemaining = currentBlock.end - currentBlock.start;
            checkState(decodedRemaining > 0, "Raw Avro block is empty.");
            currentRows =
                    currentRawBlock.toRows(
                            encodedRecordsCompatible
                                    ? ProjectedManifestEntry.ENTRY_LAYOUT_PROJECTION.projectedType()
                                    : ManifestEntry.MANIFEST_ROW_TYPE);
            checkState(currentRows.hasNext(), "Manifest block cannot be decompressed.");
            currentSourceRow = currentRows.next();
            currentEntry =
                    encodedRecordsCompatible
                            ? projectedEntry.replace(currentSourceRow)
                            : fullEntry.replace(currentSourceRow);
            decodedRemaining--;
            key.replace(currentEntry, partitions);
            checkState(
                    minor || deletes.copyable(currentEntry, identifier, false),
                    "Copyable manifest block contains a filtered entry.");
            current = true;
            metadata.replace(
                    key.kind,
                    partitions.partition(key.partitionId),
                    currentEntry.bucket(),
                    currentEntry.file().level(),
                    currentEntry.file().schemaId(),
                    key.firstRowId,
                    currentEntry.file().rowCount());
            blockIndex++;
        }

        @Override
        public void close() throws Exception {
            if (closed) {
                return;
            }
            closed = true;
            current = false;
            currentRawBlock = null;
            currentRows = null;
            currentSourceRow = null;
            currentEntry = null;
            projectedEntry.clear();
            fullEntry.clear();
            identifier.release();
            currentBlock = null;
            rawBlock = false;
            key.clear();
            reader.close();
        }
    }

    static final class InMemoryManifestCursor implements Cursor {

        final List<StoredEntry> entries;
        final ProjectedManifestEntry current =
                ProjectedManifestEntry.fullProjection().createEntry();
        final ReusableIdentifier identifier = new ReusableIdentifier();
        int position = -1;

        InMemoryManifestCursor(
                ManifestFile manifestFile,
                ManifestFileMeta meta,
                CollectedDeletes deletes,
                boolean minor,
                ManifestEntryRunMerge.SortPartitionDictionary partitions)
                throws Exception {
            long entryCount = meta.numAddedFiles() + meta.numDeletedFiles();
            this.entries = new ArrayList<>((int) entryCount);
            InternalRowSerializer serializer =
                    new InternalRowSerializer(ManifestEntry.MANIFEST_ROW_TYPE);
            ProjectedManifestEntry view = ProjectedManifestEntry.fullProjection().createEntry();
            try (CloseableIterator<ProjectedManifestEntry> iterator =
                    manifestFile.scan(meta.fileName(), ProjectedManifestEntry.fullProjection())) {
                while (iterator.hasNext()) {
                    ProjectedManifestEntry entry = iterator.next();
                    if (!minor && !deletes.copyable(entry, identifier, false)) {
                        continue;
                    }
                    BinaryRow row = serializer.toBinaryRow(entry.fullRow()).copy();
                    entries.add(
                            new StoredEntry(
                                    row,
                                    ManifestEntryRunMerge.SortKey.viewOf(
                                            view.replace(row), partitions)));
                }
            }
            entries.sort(
                    (left, right) -> ManifestEntryRunMerge.compareMergeKeys(left.key, right.key));
            view.clear();
        }

        @Override
        public boolean advance() {
            position++;
            if (position >= entries.size()) {
                current.clear();
                return false;
            }
            StoredEntry stored = entries.get(position);
            current.replace(stored.row);
            return true;
        }

        @Override
        public boolean hasCurrent() {
            return position >= 0 && position < entries.size();
        }

        @Override
        public ProjectedManifestEntry current() {
            return current;
        }

        @Override
        public EncodedEntry metadata() {
            return null;
        }

        @Override
        public ManifestEntryRunMerge.SortKey key() {
            return entries.get(position).key;
        }

        @Override
        public ByteBuffer encodedRecord() {
            return null;
        }

        @Override
        public ReusableIdentifier identifier() {
            return identifier.replaceWithPartition(current);
        }

        @Override
        public void close() {
            current.clear();
            identifier.release();
            entries.clear();
            position = -1;
        }
    }

    private static final class StoredEntry {

        final BinaryRow row;
        final ManifestEntryRunMerge.SortKey key;

        StoredEntry(BinaryRow row, ManifestEntryRunMerge.SortKey key) {
            this.row = row;
            this.key = key;
        }
    }

    /** Fixed-size tournament tree which selects a cursor with one comparison per tree level. */
    private static final class SelectionTree {

        final List<Cursor> cursors;
        final int leafBase;
        final int[] winners;

        SelectionTree(List<Cursor> cursors) {
            this.cursors = cursors;
            int base = 1;
            while (base < cursors.size()) {
                base <<= 1;
            }
            this.leafBase = base;
            this.winners = new int[leafBase << 1];
            Arrays.fill(winners, -1);
            for (int cursor = 0; cursor < cursors.size(); cursor++) {
                if (cursors.get(cursor).hasCurrent()) {
                    winners[leafBase + cursor] = cursor;
                }
            }
            for (int node = leafBase - 1; node > 0; node--) {
                winners[node] = select(winners[node << 1], winners[(node << 1) + 1]);
            }
        }

        int winner() {
            return winners[1];
        }

        Cursor cursor(int index) {
            return cursors.get(index);
        }

        void update(int cursor, boolean hasCurrent) {
            int node = leafBase + cursor;
            winners[node] = hasCurrent ? cursor : -1;
            while ((node >>= 1) > 0) {
                winners[node] = select(winners[node << 1], winners[(node << 1) + 1]);
            }
        }

        int select(int left, int right) {
            if (left < 0) {
                return right;
            }
            if (right < 0) {
                return left;
            }
            int comparison =
                    ManifestEntryRunMerge.compareMergeKeys(
                            cursors.get(left).key(), cursors.get(right).key());
            return comparison < 0 || (comparison == 0 && left < right) ? left : right;
        }

        boolean blockPrecedesOthers(int cursor, ManifestEntryRunMerge.SortKey blockLastKey) {
            for (int other = 0; other < cursors.size(); other++) {
                if (other == cursor || !cursors.get(other).hasCurrent()) {
                    continue;
                }
                int comparison =
                        ManifestEntryRunMerge.compareMergeKeys(
                                blockLastKey, cursors.get(other).key());
                if (comparison > 0 || (comparison == 0 && cursor > other)) {
                    return false;
                }
            }
            return true;
        }
    }
}
