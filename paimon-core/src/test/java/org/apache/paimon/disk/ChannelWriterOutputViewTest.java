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
import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.data.serializer.BinaryRowSerializer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChannelWriterOutputView}. */
public class ChannelWriterOutputViewTest {

    private static final int BLOCK_SIZE = 64 * 1024;

    @TempDir Path tempDir;

    @Test
    public void testEmptyOutput() throws Exception {
        BlockCompressionFactory compressionFactory =
                BlockCompressionFactory.create(BlockCompressionType.LZ4);
        try (IOManager ioManager = IOManager.create(tempDir.toString())) {
            FileIOChannel.ID channel = ioManager.createChannel();
            ChannelWriterOutputView output =
                    FileChannelUtil.createOutputView(
                            ioManager, channel, compressionFactory, BLOCK_SIZE);
            output.close();

            ChannelReaderInputView input =
                    new ChannelReaderInputView(
                            channel,
                            ioManager,
                            compressionFactory,
                            BLOCK_SIZE,
                            output.getBlockCount());
            try {
                assertThat(
                                new ChannelReaderInputViewIterator(
                                                input, null, new BinaryRowSerializer(1))
                                        .next())
                        .isNull();
                assertThat(output.getBlockCount()).isZero();
            } finally {
                input.getChannel().closeAndDelete();
            }
        }
    }
}
