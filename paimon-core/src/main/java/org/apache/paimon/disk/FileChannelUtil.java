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

package org.apache.paimon.disk;

import org.apache.paimon.compression.BlockCompressionFactory;

import java.io.IOException;
import java.util.List;

/** File channel util for runtime. */
public class FileChannelUtil {

    public static ChannelReaderInputView createInputView(
            IOManager ioManager,
            ChannelWithMeta channel,
            List<FileIOChannel> channels,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize)
            throws IOException {
        ChannelReaderInputView in =
                new ChannelReaderInputView(
                        channel.getChannel(),
                        ioManager,
                        compressionCodecFactory,
                        compressionBlockSize,
                        channel.getBlockCount());
        channels.add(in.getChannel());
        return in;
    }

    public static ChannelWriterOutputView createOutputView(
            IOManager ioManager,
            FileIOChannel.ID channel,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize)
            throws IOException {
        BufferFileWriter bufferWriter = ioManager.createBufferFileWriter(channel);
        return new ChannelWriterOutputView(
                bufferWriter, compressionCodecFactory, compressionBlockSize);
    }
}
