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

package org.apache.paimon.spark.comet;

import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link org.apache.parquet.io.SeekableInputStream} adapter for Paimon's {@link
 * SeekableInputStream}. This is used to bridge Paimon's file system with Spark's Parquet reader
 * (which uses unshaded Parquet).
 */
public class SparkParquetInputStream extends org.apache.parquet.io.SeekableInputStream {
    private final SeekableInputStream stream;

    public SparkParquetInputStream(SeekableInputStream stream) {
        this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
        return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
        stream.seek(newPos);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int start, int len) throws IOException {
        int read = 0;
        while (read < len) {
            int n = stream.read(bytes, start + read, len - read);
            if (n < 0) {
                throw new IOException("Unexpected end of stream");
            }
            read += n;
        }
    }

    @Override
    public int read() throws IOException {
        return stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    @Override
    public void readFully(ByteBuffer buf) throws IOException {
        if (buf.hasArray()) {
            readFully(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            buf.position(buf.limit());
        } else {
            byte[] bytes = new byte[buf.remaining()];
            readFully(bytes);
            buf.put(bytes);
        }
    }

    @Override
    public int read(ByteBuffer buf) throws IOException {
        if (buf.hasArray()) {
            int read =
                    stream.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
            if (read > 0) {
                buf.position(buf.position() + read);
            }
            return read;
        } else {
            byte[] bytes = new byte[buf.remaining()];
            int read = stream.read(bytes);
            if (read > 0) {
                buf.put(bytes, 0, read);
            }
            return read;
        }
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }
}
