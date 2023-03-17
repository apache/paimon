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

import org.apache.paimon.memory.Buffer;
import org.apache.paimon.utils.FileIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/** A synchronous {@link BufferFileWriter} implementation. */
public class BufferFileWriterImpl extends AbstractFileIOChannel implements BufferFileWriter {

    protected BufferFileWriterImpl(ID channelID) throws IOException {
        super(channelID, true);
    }

    @Override
    public void writeBlock(Buffer buffer) throws IOException {
        ByteBuffer nioBufferReadable = buffer.getMemorySegment().wrap(0, buffer.getSize()).slice();
        ByteBuffer header = ByteBuffer.allocateDirect(4);
        header.putInt(nioBufferReadable.remaining());
        header.flip();

        FileIOUtils.writeCompletely(fileChannel, header);
        FileIOUtils.writeCompletely(fileChannel, nioBufferReadable);
    }
}
