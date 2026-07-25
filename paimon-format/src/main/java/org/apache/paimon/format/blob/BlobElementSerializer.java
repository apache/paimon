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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/** Creates file-lifetime readers and writers for one blob field format. */
interface BlobElementSerializer {

    Writer createWriter(
            PositionOutputStream out,
            String blobFieldName,
            @Nullable BlobConsumer writeConsumer,
            boolean writeNullOnMissingFile,
            boolean writeNullOnFetchFailure,
            BlobFetchMetricReporter blobFetchMetricReporter,
            int copyBufferSize);

    boolean requiresReadInputStream(boolean blobAsDescriptor);

    /** Creates a reader which owns and closes the input stream when non-null. */
    Reader createReader(
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor);

    /** Helper method to close InputStream early if element reader doesn't need it. */
    static BlobElementSerializer.Reader createReader(
            BlobElementSerializer elementSerializer,
            FileIO fileIO,
            Path filePath,
            @Nullable SeekableInputStream in,
            boolean blobAsDescriptor) {
        boolean requiresInputStream = elementSerializer.requiresReadInputStream(blobAsDescriptor);

        if (!requiresInputStream) {
            IOUtils.closeQuietly(in);
            in = null;
        }

        return elementSerializer.createReader(fileIO, filePath, in, blobAsDescriptor);
    }

    /** Writer used for the lifetime of one output file. */
    interface Writer {

        long write(InternalRow row) throws IOException;

        void setFile(Path file);
    }

    /** Reader used for the lifetime of one input file. */
    interface Reader extends Closeable {

        Object placeholder();

        Object read(long payloadPosition, long payloadLength);
    }
}
