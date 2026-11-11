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

package org.apache.paimon.io;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/** Uses sparse local files for large zero-filled test index payloads. */
public class SparseFileIndexIO extends LocalFileIO {

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        if (!path.getName().endsWith(DataFilePathFactory.INDEX_PATH_SUFFIX)) {
            return super.newOutputStream(path, overwrite);
        }
        File file = new File(path.toUri());
        file.getParentFile().mkdirs();
        RandomAccessFile output = new RandomAccessFile(file, "rw");
        output.setLength(0);
        return new PositionOutputStream() {
            private long position;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int value) throws IOException {
                output.write(value);
                position++;
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                write(bytes, 0, bytes.length);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                boolean zeros = length >= 8192;
                for (int i = offset; zeros && i < offset + length; i++) {
                    zeros = bytes[i] == 0;
                }
                if (zeros) {
                    output.seek(position + length);
                } else {
                    output.write(bytes, offset, length);
                }
                position += length;
            }

            @Override
            public void flush() {}

            @Override
            public void close() throws IOException {
                output.setLength(position);
                output.close();
            }
        };
    }
}
