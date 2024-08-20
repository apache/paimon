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

package org.apache.paimon.sort;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.codegen.CodeGenUtils.newNormalizedKeyComputer;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;

/** A spillable {@link SortBuffer}. */
public class BinaryExternalSortBuffer implements SortBuffer {

    private final BinaryRowSerializer serializer;
    private final BinaryInMemorySortBuffer inMemorySortBuffer;
    private final IOManager ioManager;
    private SpillChannelManager channelManager;
    private final int maxNumFileHandles;
    private final BlockCompressionFactory compressionCodecFactory;
    private final int compressionBlockSize;
    private final BinaryExternalMerger merger;

    private final FileIOChannel.Enumerator enumerator;
    private final List<ChannelWithMeta> spillChannelIDs;
    private final MemorySize maxDiskSize;

    private int numRecords = 0;

    public BinaryExternalSortBuffer(
            BinaryRowSerializer serializer,
            RecordComparator comparator,
            int pageSize,
            BinaryInMemorySortBuffer inMemorySortBuffer,
            IOManager ioManager,
            int maxNumFileHandles,
            String compression,
            MemorySize maxDiskSize) {
        this.serializer = serializer;
        this.inMemorySortBuffer = inMemorySortBuffer;
        this.ioManager = ioManager;
        this.channelManager = new SpillChannelManager();
        this.maxNumFileHandles = maxNumFileHandles;
        this.compressionCodecFactory = BlockCompressionFactory.create(compression);
        this.compressionBlockSize = (int) MemorySize.parse("64 kb").getBytes();
        this.maxDiskSize = maxDiskSize;
        this.merger =
                new BinaryExternalMerger(
                        ioManager,
                        pageSize,
                        maxNumFileHandles,
                        channelManager,
                        serializer.duplicate(),
                        comparator,
                        compressionCodecFactory,
                        compressionBlockSize);
        this.enumerator = ioManager.createChannelEnumerator();
        this.spillChannelIDs = new ArrayList<>();
    }

    public static BinaryExternalSortBuffer create(
            IOManager ioManager,
            RowType rowType,
            int[] keyFields,
            long bufferSize,
            int pageSize,
            int maxNumFileHandles,
            String compression,
            MemorySize maxDiskSize) {
        return create(
                ioManager,
                rowType,
                keyFields,
                new HeapMemorySegmentPool(bufferSize, pageSize),
                maxNumFileHandles,
                compression,
                maxDiskSize);
    }

    public static BinaryExternalSortBuffer create(
            IOManager ioManager,
            RowType rowType,
            int[] keyFields,
            MemorySegmentPool pool,
            int maxNumFileHandles,
            String compression,
            MemorySize maxDiskSize) {
        RecordComparator comparator = newRecordComparator(rowType.getFieldTypes(), keyFields);
        BinaryInMemorySortBuffer sortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        newNormalizedKeyComputer(rowType.getFieldTypes(), keyFields),
                        new InternalRowSerializer(rowType),
                        comparator,
                        pool);
        return new BinaryExternalSortBuffer(
                new BinaryRowSerializer(rowType.getFieldCount()),
                comparator,
                pool.pageSize(),
                sortBuffer,
                ioManager,
                maxNumFileHandles,
                compression,
                maxDiskSize);
    }

    @Override
    public int size() {
        return numRecords;
    }

    @Override
    public void clear() {
        this.numRecords = 0;
        // release memory
        inMemorySortBuffer.clear();
        spillChannelIDs.clear();
        // delete files
        channelManager.close();
        channelManager = new SpillChannelManager();
    }

    @Override
    public long getOccupancy() {
        return inMemorySortBuffer.getOccupancy();
    }

    @Override
    public boolean flushMemory() throws IOException {
        boolean isFull = getDiskUsage() >= maxDiskSize.getBytes();
        if (isFull) {
            return false;
        } else {
            spill();
            return true;
        }
    }

    private long getDiskUsage() {
        long bytes = 0;

        for (ChannelWithMeta spillChannelID : spillChannelIDs) {
            bytes += spillChannelID.getNumBytes();
        }
        return bytes;
    }

    @VisibleForTesting
    public void write(MutableObjectIterator<BinaryRow> iterator) throws IOException {
        BinaryRow row = serializer.createInstance();
        while ((row = iterator.next(row)) != null) {
            write(row);
        }
    }

    @Override
    public boolean write(InternalRow record) throws IOException {
        while (true) {
            boolean success = inMemorySortBuffer.write(record);
            if (success) {
                this.numRecords++;
                return true;
            }
            if (inMemorySortBuffer.isEmpty()) {
                // did not fit in a fresh buffer, must be large...
                throw new IOException("The record exceeds the maximum size of a sort buffer.");
            } else {
                spill();

                if (spillChannelIDs.size() >= maxNumFileHandles) {
                    List<ChannelWithMeta> merged = merger.mergeChannelList(spillChannelIDs);
                    spillChannelIDs.clear();
                    spillChannelIDs.addAll(merged);
                }
            }
        }
    }

    @Override
    public final MutableObjectIterator<BinaryRow> sortedIterator() throws IOException {
        if (spillChannelIDs.isEmpty()) {
            return inMemorySortBuffer.sortedIterator();
        }
        return spilledIterator();
    }

    private MutableObjectIterator<BinaryRow> spilledIterator() throws IOException {
        spill();

        List<FileIOChannel> openChannels = new ArrayList<>();
        BinaryMergeIterator<BinaryRow> iterator =
                merger.getMergingIterator(spillChannelIDs, openChannels);
        channelManager.addOpenChannels(openChannels);

        return new MutableObjectIterator<BinaryRow>() {
            @Override
            public BinaryRow next(BinaryRow reuse) throws IOException {
                // BinaryMergeIterator ignore reuse object argument, use its own reusing object
                return next();
            }

            @Override
            public BinaryRow next() throws IOException {
                BinaryRow row = iterator.next();
                // BinaryMergeIterator reuse object anyway, here we need to copy it to do compaction
                return row == null ? null : row.copy();
            }
        };
    }

    private void spill() throws IOException {
        if (inMemorySortBuffer.isEmpty()) {
            return;
        }

        // open next channel
        FileIOChannel.ID channel = enumerator.next();
        channelManager.addChannel(channel);

        ChannelWriterOutputView output = null;
        int blockCount;

        try {
            output =
                    FileChannelUtil.createOutputView(
                            ioManager, channel, compressionCodecFactory, compressionBlockSize);
            new QuickSort().sort(inMemorySortBuffer);
            inMemorySortBuffer.writeToOutput(output);
            output.close();
            blockCount = output.getBlockCount();
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        }

        spillChannelIDs.add(new ChannelWithMeta(channel, blockCount, output.getWriteBytes()));
        inMemorySortBuffer.clear();
    }
}
