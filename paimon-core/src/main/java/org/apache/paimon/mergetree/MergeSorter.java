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

package org.apache.paimon.mergetree;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelReaderInputViewIterator;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.CachelessSegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** The merge sorter to sort and merge readers with key overlap. */
public class MergeSorter {

    private final RowType keyType;
    private RowType valueType;

    private final SortEngine sortEngine;
    private final int spillThreshold;
    private final String compression;

    private final MemorySegmentPool memoryPool;

    @Nullable private IOManager ioManager;

    public MergeSorter(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.sortEngine = options.sortEngine();
        this.spillThreshold = options.sortSpillThreshold();
        this.compression = options.spillCompression();
        this.keyType = keyType;
        this.valueType = valueType;
        this.memoryPool =
                new CachelessSegmentPool(options.sortSpillBufferSize(), options.pageSize());
        this.ioManager = ioManager;
    }

    public MemorySegmentPool memoryPool() {
        return memoryPool;
    }

    public RowType valueType() {
        return valueType;
    }

    public void setIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
    }

    public void setProjectedValueType(RowType projectedType) {
        this.valueType = projectedType;
    }

    public <T> RecordReader<T> mergeSort(
            List<SizedReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        if (ioManager != null && lazyReaders.size() > spillThreshold) {
            return spillMergeSort(
                    lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
        }

        return mergeSortNoSpill(
                lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    public <T> RecordReader<T> mergeSortNoSpill(
            List<? extends ReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>(lazyReaders.size());
        for (ReaderSupplier<KeyValue> supplier : lazyReaders) {
            try {
                readers.add(supplier.get());
            } catch (IOException e) {
                // if one of the readers creating failed, we need to close them all.
                readers.forEach(IOUtils::closeQuietly);
                throw e;
            }
        }

        return SortMergeReader.createSortMergeReader(
                readers, keyComparator, userDefinedSeqComparator, mergeFunction, sortEngine);
    }

    private <T> RecordReader<T> spillMergeSort(
            List<SizedReaderSupplier<KeyValue>> inputReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        List<SizedReaderSupplier<KeyValue>> sortedReaders = new ArrayList<>(inputReaders);
        sortedReaders.sort(Comparator.comparingLong(SizedReaderSupplier::estimateSize));
        int spillSize = inputReaders.size() - spillThreshold;

        List<ReaderSupplier<KeyValue>> readers =
                new ArrayList<>(sortedReaders.subList(spillSize, sortedReaders.size()));
        for (ReaderSupplier<KeyValue> supplier : sortedReaders.subList(0, spillSize)) {
            readers.add(spill(supplier));
        }

        return mergeSortNoSpill(readers, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    private ReaderSupplier<KeyValue> spill(ReaderSupplier<KeyValue> readerSupplier)
            throws IOException {
        checkArgument(ioManager != null);

        FileIOChannel.ID channel = ioManager.createChannel();
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        BlockCompressionFactory compressFactory = BlockCompressionFactory.create(compression);
        int compressBlock = (int) MemorySize.parse("64 kb").getBytes();

        ChannelWithMeta channelWithMeta;
        ChannelWriterOutputView out =
                FileChannelUtil.createOutputView(
                        ioManager, channel, compressFactory, compressBlock);
        try (RecordReader<KeyValue> reader = readerSupplier.get(); ) {
            RecordIterator<KeyValue> batch;
            KeyValue record;
            while ((batch = reader.readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    serializer.serialize(record, out);
                }
                batch.releaseBatch();
            }
        } finally {
            out.close();
            channelWithMeta =
                    new ChannelWithMeta(channel, out.getBlockCount(), out.getWriteBytes());
        }

        return new SpilledReaderSupplier(
                channelWithMeta, compressFactory, compressBlock, serializer);
    }

    private class SpilledReaderSupplier implements ReaderSupplier<KeyValue> {

        private final ChannelWithMeta channel;
        private final BlockCompressionFactory compressFactory;
        private final int compressBlock;
        private final KeyValueWithLevelNoReusingSerializer serializer;

        public SpilledReaderSupplier(
                ChannelWithMeta channel,
                BlockCompressionFactory compressFactory,
                int compressBlock,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.channel = channel;
            this.compressFactory = compressFactory;
            this.compressBlock = compressBlock;
            this.serializer = serializer;
        }

        @Override
        public RecordReader<KeyValue> get() throws IOException {
            ChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager, channel, new ArrayList<>(), compressFactory, compressBlock);
            BinaryRowSerializer rowSerializer = new BinaryRowSerializer(serializer.numFields());
            ChannelReaderInputViewIterator iterator =
                    new ChannelReaderInputViewIterator(view, null, rowSerializer);
            return new ChannelReaderReader(view, iterator, serializer);
        }
    }

    private static class ChannelReaderReader implements RecordReader<KeyValue> {

        private final ChannelReaderInputView view;
        private final ChannelReaderInputViewIterator iterator;
        private final KeyValueWithLevelNoReusingSerializer serializer;

        private ChannelReaderReader(
                ChannelReaderInputView view,
                ChannelReaderInputViewIterator iterator,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.view = view;
            this.iterator = iterator;
            this.serializer = serializer;
        }

        private boolean read = false;

        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (read) {
                return null;
            }

            read = true;
            return new RecordIterator<KeyValue>() {
                @Override
                public KeyValue next() throws IOException {
                    BinaryRow noReuseRow = iterator.next();
                    if (noReuseRow == null) {
                        return null;
                    }
                    return serializer.fromRow(noReuseRow);
                }

                @Override
                public void releaseBatch() {}
            };
        }

        @Override
        public void close() throws IOException {
            view.getChannel().closeAndDelete();
        }
    }
}
