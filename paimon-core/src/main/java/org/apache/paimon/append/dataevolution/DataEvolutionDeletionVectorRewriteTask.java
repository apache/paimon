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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.VersionedSerializer;
import org.apache.paimon.deletionvectors.Bitmap64DeletionVector;
import org.apache.paimon.deletionvectors.BitmapDeletionVector;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.paimon.table.BucketMode.UNAWARE_BUCKET;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.readCount;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * A partition-local batch of complete anchor rewrites and uniquely owned old index files.
 *
 * <p>New anchor bitmaps share rolling output files. Retained entries are written separately for
 * each old index, preserving the conflict boundaries of subsequent deletes.
 */
public class DataEvolutionDeletionVectorRewriteTask {

    final BinaryRow partition;
    final List<Rewrite> rewrites = new ArrayList<>();
    final List<IndexRewrite> indexes = new ArrayList<>();

    DataEvolutionDeletionVectorRewriteTask(BinaryRow partition) {
        this.partition = partition;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int workCount() {
        return rewrites.size() + indexes.size();
    }

    public CommitMessage execute(FileStoreTable table) {
        DeletionVectorsIndexFile index =
                table.store().newIndexFileHandler().dvIndex(partition, UNAWARE_BUCKET);
        boolean bitmap64 = table.coreOptions().deletionVectorBitmap64();
        Iterator<Map.Entry<String, DeletionVector>> bitmaps =
                new Iterator<Map.Entry<String, DeletionVector>>() {
                    private int rewritePosition;
                    private Iterator<Map.Entry<String, DeletionVector>> current =
                            Collections.emptyIterator();

                    @Override
                    public boolean hasNext() {
                        while (!current.hasNext()) {
                            if (rewritePosition < rewrites.size()) {
                                current =
                                        rewrite(rewrites.get(rewritePosition++), index, bitmap64)
                                                .entrySet()
                                                .iterator();
                            } else {
                                return false;
                            }
                        }
                        return true;
                    }

                    @Override
                    public Map.Entry<String, DeletionVector> next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                        return current.next();
                    }
                };
        List<IndexFileMeta> added = new ArrayList<>();
        try {
            added.addAll(index.writeWithRolling(bitmaps));
            for (IndexRewrite old : indexes) {
                Map<String, DeletionFile> retained =
                        index.toDeletionFiles(Collections.singletonList(old.file));
                old.removed.forEach(retained::remove);
                added.addAll(index.writeWithRolling(index.readDeletionVector(retained)));
            }
        } catch (RuntimeException failure) {
            for (IndexFileMeta file : added) {
                try {
                    table.fileIO().deleteQuietly(index.path(file));
                } catch (RuntimeException cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
            }
            throw failure;
        }
        List<IndexFileMeta> deleted = new ArrayList<>(indexes.size());
        indexes.forEach(old -> deleted.add(old.file));
        return new CommitMessageImpl(
                partition,
                UNAWARE_BUCKET,
                null,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        added,
                        deleted));
    }

    private static Map<String, DeletionVector> rewrite(
            Rewrite rewrite, DeletionVectorsIndexFile index, boolean bitmap64) {
        Map<String, Map<String, DeletionFile>> byIndex = new LinkedHashMap<>();
        Map<String, Range> inputRanges = new LinkedHashMap<>();
        for (Source source : rewrite.sources) {
            byIndex.computeIfAbsent(source.deletion.path(), ignored -> new LinkedHashMap<>())
                    .put(source.anchor, source.deletion);
            inputRanges.put(source.anchor, source.range);
        }
        TreeMap<Long, Map.Entry<String, Range>> outputs = new TreeMap<>();
        rewrite.outputs.entrySet().forEach(output -> outputs.put(output.getValue().from, output));
        Map<String, DeletionVector> result = new LinkedHashMap<>();
        for (Map<String, DeletionFile> inputs : byIndex.values()) {
            index.readDeletionVector(inputs)
                    .forEach(
                            (anchor, bitmap) ->
                                    move(
                                            bitmap,
                                            inputRanges.get(anchor),
                                            outputs,
                                            result,
                                            bitmap64));
        }
        return result;
    }

    static void move(
            DeletionVector bitmap,
            Range input,
            TreeMap<Long, Map.Entry<String, Range>> outputs,
            Map<String, DeletionVector> result,
            boolean bitmap64) {
        if (bitmap.isEmpty()) {
            return;
        }
        Map.Entry<String, Range> first = outputs.floorEntry(input.from).getValue();
        boolean sameFormat =
                bitmap64
                        ? bitmap instanceof Bitmap64DeletionVector
                        : bitmap instanceof BitmapDeletionVector;
        if (input.equals(first.getValue()) && sameFormat) {
            // Disjoint input ranges make this output exclusive to the current anchor.
            result.put(first.getKey(), bitmap);
            return;
        }
        bitmap.forEachDeletedPosition(
                position -> {
                    long rowId = input.from + position;
                    Map.Entry<String, Range> output = outputs.floorEntry(rowId).getValue();
                    long offset = rowId - output.getValue().from;
                    checkState(
                            offset >= 0 && offset < output.getValue().count(),
                            "Deleted row ID %s is outside output range %s.",
                            rowId,
                            output.getValue());
                    result.computeIfAbsent(output.getKey(), ignored -> newBitmap(bitmap64))
                            .delete(offset);
                });
    }

    private static DeletionVector newBitmap(boolean bitmap64) {
        return bitmap64 ? new Bitmap64DeletionVector() : new BitmapDeletionVector();
    }

    static class Source {
        final String anchor;
        final Range range;
        final DeletionFile deletion;

        Source(String anchor, Range range, DeletionFile deletion) {
            this.anchor = anchor;
            this.range = range;
            this.deletion = deletion;
        }
    }

    static class Rewrite {
        final List<Source> sources;
        final Map<String, Range> outputs;

        Rewrite(List<Source> sources, Map<String, Range> outputs) {
            this.sources = sources;
            this.outputs = outputs;
        }
    }

    static class IndexRewrite {
        final IndexFileMeta file;
        final Set<String> removed;

        IndexRewrite(IndexFileMeta file, Set<String> removed) {
            this.file = file;
            this.removed = removed;
        }
    }

    /** Runtime-only task format shared by Flink and Spark. */
    public static class Serializer
            implements VersionedSerializer<DataEvolutionDeletionVectorRewriteTask> {

        private final IndexFileMetaSerializer indexes = new IndexFileMetaSerializer();

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(DataEvolutionDeletionVectorRewriteTask task) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(1024);
            serializeBinaryRow(task.partition, out);
            out.writeInt(task.rewrites.size());
            for (Rewrite rewrite : task.rewrites) {
                out.writeInt(rewrite.sources.size());
                for (Source source : rewrite.sources) {
                    out.writeUTF(source.anchor);
                    out.writeLong(source.range.from);
                    out.writeLong(source.range.to);
                    DeletionFile.serialize(out, source.deletion);
                }
                out.writeInt(rewrite.outputs.size());
                for (Map.Entry<String, Range> output : rewrite.outputs.entrySet()) {
                    out.writeUTF(output.getKey());
                    out.writeLong(output.getValue().from);
                    out.writeLong(output.getValue().to);
                }
            }
            out.writeInt(task.indexes.size());
            for (IndexRewrite index : task.indexes) {
                indexes.serializeList(Collections.singletonList(index.file), out);
                out.writeInt(index.removed.size());
                for (String anchor : index.removed) {
                    out.writeUTF(anchor);
                }
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public DataEvolutionDeletionVectorRewriteTask deserialize(int version, byte[] bytes)
                throws IOException {
            checkArgument(
                    version == getVersion(), "Unsupported DV rewrite task version %s.", version);
            DataInputDeserializer in = new DataInputDeserializer(bytes);
            DataEvolutionDeletionVectorRewriteTask task =
                    new DataEvolutionDeletionVectorRewriteTask(deserializeBinaryRow(in));
            int rewriteCount = readCount(in, "DV rewrites");
            for (int i = 0; i < rewriteCount; i++) {
                int sourceCount = readCount(in, "DV sources");
                List<Source> sources = new ArrayList<>();
                for (int j = 0; j < sourceCount; j++) {
                    sources.add(
                            new Source(
                                    in.readUTF(),
                                    new Range(in.readLong(), in.readLong()),
                                    DeletionFile.deserialize(in)));
                }
                int outputCount = readCount(in, "DV outputs");
                Map<String, Range> outputs = new LinkedHashMap<>();
                for (int j = 0; j < outputCount; j++) {
                    outputs.put(in.readUTF(), new Range(in.readLong(), in.readLong()));
                }
                task.rewrites.add(new Rewrite(sources, outputs));
            }
            int indexCount = readCount(in, "DV index rewrites");
            for (int i = 0; i < indexCount; i++) {
                List<IndexFileMeta> files = indexes.deserializeList(in);
                checkArgument(files.size() == 1, "Expected one owned DV index file.");
                int removedCount = readCount(in, "Removed DV anchors");
                Set<String> removed = new HashSet<>();
                for (int j = 0; j < removedCount; j++) {
                    removed.add(in.readUTF());
                }
                task.indexes.add(new IndexRewrite(files.get(0), removed));
            }
            return task;
        }
    }
}
