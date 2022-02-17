package org.apache.flink.table.store.connector.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.MergeTreeReaderFactory;
import org.apache.flink.table.store.file.mergetree.MergeTreeWriterFactory;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.operation.FileStoreReadImpl;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static java.util.Collections.singletonList;

public class TestDataReadWrite {

    private static final RowType KEY_TYPE =
            new RowType(singletonList(new RowType.RowField("k", new IntType())));
    private static final RowType VALUE_TYPE =
            new RowType(singletonList(new RowType.RowField("v", new IntType())));
    private static final Comparator<RowData> COMPARATOR = Comparator.comparingInt(o -> o.getInt(0));

    private final FileFormat avro;
    private final FileStorePathFactory pathFactory;
    private final ExecutorService service;

    public TestDataReadWrite(String root, ExecutorService service) {
        this.avro =
                FileFormat.fromIdentifier(
                        Thread.currentThread().getContextClassLoader(),
                        "avro",
                        new Configuration());
        this.pathFactory =
                new FileStorePathFactory(new Path(root), RowType.of(new IntType()), "default");
        this.service = service;
    }

    public FileStoreRead createRead() {
        MergeTreeReaderFactory factory =
                new MergeTreeReaderFactory(
                        KEY_TYPE,
                        VALUE_TYPE,
                        COMPARATOR,
                        new DeduplicateAccumulator(),
                        avro,
                        pathFactory);
        return new FileStoreReadImpl(factory);
    }

    public List<SstFileMeta> writeFiles(
            BinaryRowData partition, int bucket, List<Tuple2<Integer, Integer>> kvs)
            throws Exception {
        Preconditions.checkNotNull(
                service, "ExecutorService must be provided if writeFiles is needed");
        RecordWriter writer = createMergeTreeWriter(partition, bucket);
        for (Tuple2<Integer, Integer> tuple2 : kvs) {
            writer.write(ValueKind.ADD, GenericRowData.of(tuple2.f0), GenericRowData.of(tuple2.f1));
        }
        List<SstFileMeta> files = writer.prepareCommit().newFiles();
        writer.close();
        return new ArrayList<>(files);
    }

    private RecordWriter createMergeTreeWriter(BinaryRowData partition, int bucket) {
        MergeTreeOptions options = new MergeTreeOptions(new Configuration());
        return new MergeTreeWriterFactory(
                        KEY_TYPE,
                        VALUE_TYPE,
                        COMPARATOR,
                        new DeduplicateAccumulator(),
                        avro,
                        pathFactory,
                        options)
                .create(partition, bucket, Collections.emptyList(), service);
    }
}
