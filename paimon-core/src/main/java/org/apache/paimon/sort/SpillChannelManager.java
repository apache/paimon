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

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/** Channel manager to manage the life cycle of spill channels. */
public class SpillChannelManager {

    private final HashSet<FileIOChannel.ID> channels;
    private final HashSet<FileIOChannel> openChannels;
    private long maxDiskBytes = Long.MAX_VALUE;
    private long spillBytes;

    public SpillChannelManager() {
        this.channels = new HashSet<>(64);
        this.openChannels = new HashSet<>(64);
    }

    void setMaxDiskBytes(long maxDiskBytes) {
        this.maxDiskBytes = maxDiskBytes;
    }

    synchronized long spillBytes() {
        return spillBytes;
    }

    synchronized ChannelWriterOutputView openWriter(
            IOManager ioManager,
            FileIOChannel.ID id,
            BlockCompressionFactory compression,
            int blockSize)
            throws IOException {
        channels.add(id);
        ChannelWriterOutputView output =
                FileChannelUtil.createOutputView(ioManager, id, compression, blockSize);
        if (maxDiskBytes != Long.MAX_VALUE) {
            output.setMaxFileBytes(maxDiskBytes - spillBytes);
        }
        return output;
    }

    synchronized ChannelWithMeta finishWriter(FileIOChannel.ID id, ChannelWriterOutputView output)
            throws IOException {
        output.close();
        long bytes = output.getWriteBytes();
        spillBytes += bytes;
        return new ChannelWithMeta(id, output.getBlockCount(), bytes);
    }

    /** Open File channels. */
    public synchronized void addOpenChannels(List<FileIOChannel> toOpen) {
        for (FileIOChannel channel : toOpen) {
            openChannels.add(channel);
            channels.remove(channel.getChannelID());
        }
    }

    synchronized void removeChannel(ChannelWithMeta channel) {
        // FileIOChannel deletion is best effort. Retain the charge and retry at reset if needed.
        if (maxDiskBytes != Long.MAX_VALUE && channel.getChannel().getPathFile().exists()) {
            return;
        }
        channels.remove(channel.getChannel());
        spillBytes -= channel.getNumBytes();
    }

    public synchronized void reset() {
        spillBytes = 0;
        for (Iterator<FileIOChannel> channels = this.openChannels.iterator();
                channels.hasNext(); ) {
            final FileIOChannel channel = channels.next();
            channels.remove();
            try {
                channel.closeAndDelete();
            } catch (Throwable ignored) {
            }
        }

        for (Iterator<FileIOChannel.ID> channels = this.channels.iterator(); channels.hasNext(); ) {
            final FileIOChannel.ID channel = channels.next();
            channels.remove();
            try {
                final File f = new File(channel.getPath());
                if (f.exists()) {
                    f.delete();
                }
            } catch (Throwable ignored) {
            }
        }
    }
}
