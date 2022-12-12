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

package org.apache.flink.table.store.file.sort;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.compression.Lz4BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.io.ChannelWithMeta;
import org.apache.flink.table.runtime.operators.sort.BinaryMergeIterator;
import org.apache.flink.table.runtime.operators.sort.SpillChannelManager;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.store.codegen.RecordComparator;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** A spillable {@link SortBuffer}. */
public class BinaryExternalSortBuffer implements SortBuffer {

    private final BinaryRowDataSerializer serializer;
    private final int pageSize;
    private final BinaryInMemorySortBuffer inMemorySortBuffer;
    private final IOManager ioManager;
    private SpillChannelManager channelManager;
    private final int maxNumFileHandles;
    private final boolean compressionEnable;
    private final BlockCompressionFactory compressionCodecFactory;
    private final int compressionBlockSize;
    private final BinaryExternalMerger merger;

    private final FileIOChannel.Enumerator enumerator;
    private final List<ChannelWithMeta> spillChannelIDs;

    private int numRecords = 0;

    public BinaryExternalSortBuffer(
            BinaryRowDataSerializer serializer,
            RecordComparator comparator,
            int pageSize,
            BinaryInMemorySortBuffer inMemorySortBuffer,
            IOManager ioManager,
            int maxNumFileHandles) {
        this.serializer = serializer;
        this.pageSize = pageSize;
        this.inMemorySortBuffer = inMemorySortBuffer;
        this.ioManager = ioManager;
        this.channelManager = new SpillChannelManager();
        this.maxNumFileHandles = maxNumFileHandles;
        this.compressionEnable = true;
        this.compressionCodecFactory = new Lz4BlockCompressionFactory();
        this.compressionBlockSize = (int) MemorySize.parse("64 kb").getBytes();
        this.merger =
                new BinaryExternalMerger(
                        ioManager,
                        pageSize,
                        maxNumFileHandles,
                        channelManager,
                        (BinaryRowDataSerializer) serializer.duplicate(),
                        comparator,
                        compressionEnable,
                        compressionCodecFactory,
                        compressionBlockSize);
        this.enumerator = ioManager.createChannelEnumerator();
        this.spillChannelIDs = new ArrayList<>();
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
        channelManager.close();
        // delete files
        channelManager = new SpillChannelManager();
    }

    @Override
    public long getOccupancy() {
        return inMemorySortBuffer.getOccupancy();
    }

    @Override
    public boolean flushMemory() throws IOException {
        spill();
        return true;
    }

    @VisibleForTesting
    public void write(MutableObjectIterator<BinaryRowData> iterator) throws IOException {
        BinaryRowData row = serializer.createInstance();
        while ((row = iterator.next(row)) != null) {
            write(row);
        }
    }

    @Override
    public boolean write(RowData record) throws IOException {
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
    public final MutableObjectIterator<BinaryRowData> sortedIterator() throws IOException {
        if (spillChannelIDs.isEmpty()) {
            return inMemorySortBuffer.sortedIterator();
        }
        return spilledIterator();
    }

    private MutableObjectIterator<BinaryRowData> spilledIterator() throws IOException {
        spill();

        List<FileIOChannel> openChannels = new ArrayList<>();
        BinaryMergeIterator<BinaryRowData> iterator =
                merger.getMergingIterator(spillChannelIDs, openChannels);
        channelManager.addOpenChannels(openChannels);

        return new MutableObjectIterator<BinaryRowData>() {
            @Override
            public BinaryRowData next(BinaryRowData reuse) throws IOException {
                // BinaryMergeIterator ignore reuse object argument, use its own reusing object
                return next();
            }

            @Override
            public BinaryRowData next() throws IOException {
                BinaryRowData row = iterator.next();
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

        AbstractChannelWriterOutputView output = null;
        int bytesInLastBuffer;
        int blockCount;

        try {
            output =
                    FileChannelUtil.createOutputView(
                            ioManager,
                            channel,
                            compressionEnable,
                            compressionCodecFactory,
                            compressionBlockSize,
                            pageSize);
            new QuickSort().sort(inMemorySortBuffer);
            inMemorySortBuffer.writeToOutput(output);
            bytesInLastBuffer = output.close();
            blockCount = output.getBlockCount();
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            throw e;
        }

        spillChannelIDs.add(new ChannelWithMeta(channel, blockCount, bytesInLastBuffer));
        inMemorySortBuffer.clear();
    }
}
