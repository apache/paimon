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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.store.file.FileFormat;
import org.apache.flink.table.store.file.ValueKind;
import org.apache.flink.table.store.file.mergetree.MergeTree;
import org.apache.flink.table.store.file.mergetree.MergeTreeOptions;
import org.apache.flink.table.store.file.mergetree.SortedRun;
import org.apache.flink.table.store.file.mergetree.compact.DeduplicateAccumulator;
import org.apache.flink.table.store.file.mergetree.compact.IntervalPartition;
import org.apache.flink.table.store.file.mergetree.sst.SstFile;
import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;
import org.apache.flink.table.store.file.operation.FileStoreRead;
import org.apache.flink.table.store.file.utils.FileStorePathFactory;
import org.apache.flink.table.store.file.utils.RecordReader;
import org.apache.flink.table.store.file.utils.RecordWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import scala.Tuple2;

import static java.util.Collections.singletonList;

/**
 * Test {@link FileStoreRead}.
 *
 * <p>TODO: remove this, use FileStoreReadImpl.
 */
public class TestFileStoreRead implements FileStoreRead {

    private static final Comparator<RowData> COMPARATOR = Comparator.comparingInt(o -> o.getInt(0));

    private final FileStorePathFactory pathFactory;

    private final ExecutorService service;

    public TestFileStoreRead(Path root, ExecutorService service) {
        this.pathFactory = new FileStorePathFactory(root, RowType.of(new IntType()), "default");
        this.service = service;
    }

    @Override
    public void withKeyProjection(int[][] projectedFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void withValueProjection(int[][] projectedFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RecordReader createReader(BinaryRowData partition, int bucket, List<SstFileMeta> files)
            throws IOException {
        MergeTree mergeTree = createMergeTree(partition, bucket);
        List<List<SortedRun>> runs = new IntervalPartition(files, COMPARATOR).partition();
        return mergeTree.createReader(runs, true);
    }

    private MergeTree createMergeTree(BinaryRowData partition, int bucket) {
        MergeTreeOptions options = new MergeTreeOptions(new Configuration());
        SstFile sstFile =
                new SstFile.Factory(
                                new RowType(
                                        singletonList(new RowType.RowField("k", new IntType()))),
                                new RowType(
                                        singletonList(new RowType.RowField("v", new IntType()))),
                                FileFormat.fromIdentifier(
                                        Thread.currentThread().getContextClassLoader(),
                                        "avro",
                                        new Configuration()),
                                pathFactory,
                                options.targetFileSize)
                        .create(partition, bucket);
        return new MergeTree(options, sstFile, COMPARATOR, service, new DeduplicateAccumulator());
    }

    public List<SstFileMeta> writeFiles(
            BinaryRowData partition, int bucket, List<Tuple2<Integer, Integer>> kvs)
            throws Exception {
        RecordWriter writer = createMergeTree(partition, bucket).createWriter(new ArrayList<>());
        for (Tuple2<Integer, Integer> tuple2 : kvs) {
            writer.write(ValueKind.ADD, GenericRowData.of(tuple2._1), GenericRowData.of(tuple2._2));
        }
        List<SstFileMeta> files = writer.prepareCommit().newFiles();
        writer.close();
        return new ArrayList<>(files);
    }
}
