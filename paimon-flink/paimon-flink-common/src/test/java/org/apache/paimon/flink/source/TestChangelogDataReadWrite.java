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

package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.KeyValueFileReaderFactory;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.operation.KeyValueFileStoreWrite;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonList;

/** Util class to read and write data for source tests. */
public class TestChangelogDataReadWrite {

    private static final RowType KEY_TYPE =
            new RowType(singletonList(new DataField(0, "k", new BigIntType())));
    private static final RowType VALUE_TYPE =
            new RowType(singletonList(new DataField(0, "v", new BigIntType())));
    private static final Comparator<InternalRow> COMPARATOR =
            Comparator.comparingLong(o -> o.getLong(0));
    private static final RecordEqualiser EQUALISER =
            (row1, row2) -> row1.getLong(0) == row2.getLong(0);
    private static final KeyValueFieldsExtractor EXTRACTOR =
            new KeyValueFieldsExtractor() {
                @Override
                public List<DataField> keyFields(TableSchema schema) {
                    return Collections.singletonList(
                            new DataField(0, "k", new org.apache.paimon.types.BigIntType(false)));
                }

                @Override
                public List<DataField> valueFields(TableSchema schema) {
                    return Collections.singletonList(
                            new DataField(0, "v", new org.apache.paimon.types.BigIntType(false)));
                }
            };

    private final FileFormat avro;
    private final Path tablePath;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final String commitUser;

    public TestChangelogDataReadWrite(String root) {
        this.avro = FileFormat.fromIdentifier("avro", new Options());
        this.tablePath = new Path(root);
        this.pathFactory =
                new FileStorePathFactory(
                        tablePath,
                        RowType.of(new IntType()),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue().toString());
        this.snapshotManager = new SnapshotManager(LocalFileIO.create(), new Path(root));
        this.commitUser = UUID.randomUUID().toString();
    }

    public TableRead createReadWithKey() {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        CoreOptions options = new CoreOptions(new HashMap<>());
        TableSchema schema = schemaManager.schema(0);
        MergeFileSplitRead read =
                new MergeFileSplitRead(
                        options,
                        schema,
                        KEY_TYPE,
                        VALUE_TYPE,
                        COMPARATOR,
                        DeduplicateMergeFunction.factory(),
                        KeyValueFileReaderFactory.builder(
                                LocalFileIO.create(),
                                schemaManager,
                                schema,
                                KEY_TYPE,
                                VALUE_TYPE,
                                ignore -> avro,
                                pathFactory,
                                EXTRACTOR,
                                options));
        RawFileSplitRead rawFileRead =
                new RawFileSplitRead(
                        LocalFileIO.create(),
                        schemaManager,
                        schema,
                        VALUE_TYPE,
                        FileFormatDiscover.of(options),
                        pathFactory,
                        options.fileIndexReadEnabled());
        return new KeyValueTableRead(() -> read, () -> rawFileRead, null);
    }

    public List<DataFileMeta> writeFiles(
            BinaryRow partition, int bucket, List<Tuple2<Long, Long>> kvs) throws Exception {
        RecordWriter<KeyValue> writer = createMergeTreeWriter(partition, bucket);
        for (Tuple2<Long, Long> tuple2 : kvs) {
            writer.write(
                    new KeyValue()
                            .replace(
                                    GenericRow.of(tuple2.f0),
                                    RowKind.INSERT,
                                    GenericRow.of(tuple2.f1)));
        }
        List<DataFileMeta> files = writer.prepareCommit(true).newFilesIncrement().newFiles();
        writer.close();
        return new ArrayList<>(files);
    }

    public RecordWriter<KeyValue> createMergeTreeWriter(BinaryRow partition, int bucket) {
        CoreOptions options =
                new CoreOptions(Collections.singletonMap(CoreOptions.FILE_FORMAT.key(), "avro"));

        Map<String, FileStorePathFactory> pathFactoryMap = new HashMap<>();
        pathFactoryMap.put("avro", pathFactory);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        RecordWriter<KeyValue> writer =
                new KeyValueFileStoreWrite(
                                LocalFileIO.create(),
                                schemaManager,
                                schemaManager.schema(0),
                                commitUser,
                                KEY_TYPE,
                                VALUE_TYPE,
                                () -> COMPARATOR,
                                () -> null,
                                () -> EQUALISER,
                                DeduplicateMergeFunction.factory(),
                                pathFactory,
                                pathFactoryMap,
                                snapshotManager,
                                null, // not used, we only create an empty writer
                                null,
                                null,
                                options,
                                EXTRACTOR,
                                tablePath.getName())
                        .createWriterContainer(partition, bucket, true)
                        .writer;
        ((MemoryOwner) writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        return writer;
    }
}
