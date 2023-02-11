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

package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.GenericRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.file.memory.HeapMemorySegmentPool;
import org.apache.flink.table.store.file.memory.MemoryOwner;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateMergeFunction;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreRead;
import org.apache.flink.table.store.file.operation.KeyValueFileStoreWrite;
import org.apache.flink.table.store.file.predicate.Predicate;
import org.apache.flink.table.store.file.schema.KeyValueFieldsExtractor;
import org.apache.flink.table.store.file.schema.SchemaManager;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.store.file.utils.SnapshotManager;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.fs.local.LocalFileIO;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.table.source.KeyValueTableRead;
import org.apache.flink.table.store.table.source.TableRead;
import org.apache.flink.table.store.table.source.ValueContentRowDataRecordIterator;
import org.apache.flink.table.store.table.source.ValueCountRowDataRecordIterator;
import org.apache.flink.table.store.types.BigIntType;
import org.apache.flink.table.store.types.DataField;
import org.apache.flink.table.store.types.IntType;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.types.RowType;
import org.apache.flink.table.store.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static java.util.Collections.singletonList;

/** Util class to read and write data for source tests. */
public class TestChangelogDataReadWrite {

    private static final RowType KEY_TYPE =
            new RowType(singletonList(new DataField(0, "k", new BigIntType())));
    private static final RowType VALUE_TYPE =
            new RowType(singletonList(new DataField(0, "v", new BigIntType())));
    private static final Comparator<InternalRow> COMPARATOR =
            Comparator.comparingLong(o -> o.getLong(0));
    private static final KeyValueFieldsExtractor EXTRACTOR =
            new KeyValueFieldsExtractor() {
                @Override
                public List<DataField> keyFields(TableSchema schema) {
                    return Collections.singletonList(
                            new DataField(
                                    0,
                                    "k",
                                    new org.apache.flink.table.store.types.BigIntType(false)));
                }

                @Override
                public List<DataField> valueFields(TableSchema schema) {
                    return Collections.singletonList(
                            new DataField(
                                    0,
                                    "v",
                                    new org.apache.flink.table.store.types.BigIntType(false)));
                }
            };

    private final FileFormat avro;
    private final Path tablePath;
    private final FileStorePathFactory pathFactory;
    private final SnapshotManager snapshotManager;
    private final ExecutorService service;
    private final String commitUser;

    public TestChangelogDataReadWrite(String root, ExecutorService service) {
        this.avro = FileFormat.fromIdentifier("avro", new Options());
        this.tablePath = new Path(root);
        this.pathFactory =
                new FileStorePathFactory(
                        tablePath,
                        RowType.of(new IntType()),
                        "default",
                        CoreOptions.FILE_FORMAT.defaultValue());
        this.snapshotManager = new SnapshotManager(LocalFileIO.create(), new Path(root));
        this.service = service;
        this.commitUser = UUID.randomUUID().toString();
    }

    public TableRead createReadWithKey() {
        return createRead(ValueContentRowDataRecordIterator::new);
    }

    public TableRead createReadWithValueCount() {
        return createRead(ValueCountRowDataRecordIterator::new);
    }

    private TableRead createRead(
            Function<
                            RecordReader.RecordIterator<KeyValue>,
                            RecordReader.RecordIterator<InternalRow>>
                    rowDataIteratorCreator) {
        KeyValueFileStoreRead read =
                new KeyValueFileStoreRead(
                        LocalFileIO.create(),
                        new SchemaManager(LocalFileIO.create(), tablePath),
                        0,
                        KEY_TYPE,
                        VALUE_TYPE,
                        COMPARATOR,
                        DeduplicateMergeFunction.factory(),
                        ignore -> avro,
                        pathFactory,
                        EXTRACTOR);
        return new KeyValueTableRead(read) {
            @Override
            public TableRead withFilter(Predicate predicate) {
                throw new UnsupportedOperationException();
            }

            @Override
            public TableRead withProjection(int[][] projection) {
                throw new UnsupportedOperationException();
            }

            @Override
            protected RecordReader.RecordIterator<InternalRow> rowDataRecordIteratorFromKv(
                    RecordReader.RecordIterator<KeyValue> kvRecordIterator) {
                return rowDataIteratorCreator.apply(kvRecordIterator);
            }
        };
    }

    public List<DataFileMeta> writeFiles(
            BinaryRow partition, int bucket, List<Tuple2<Long, Long>> kvs) throws Exception {
        Preconditions.checkNotNull(
                service, "ExecutorService must be provided if writeFiles is needed");
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
        RecordWriter<KeyValue> writer =
                new KeyValueFileStoreWrite(
                                LocalFileIO.create(),
                                new SchemaManager(LocalFileIO.create(), tablePath),
                                0,
                                commitUser,
                                KEY_TYPE,
                                VALUE_TYPE,
                                () -> COMPARATOR,
                                DeduplicateMergeFunction.factory(),
                                pathFactory,
                                snapshotManager,
                                null, // not used, we only create an empty writer
                                options,
                                EXTRACTOR)
                        .createEmptyWriterContainer(partition, bucket, service)
                        .writer;
        ((MemoryOwner) writer)
                .setMemoryPool(
                        new HeapMemorySegmentPool(options.writeBufferSize(), options.pageSize()));
        return writer;
    }
}
