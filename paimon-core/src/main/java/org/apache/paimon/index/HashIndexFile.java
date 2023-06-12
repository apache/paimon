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

package org.apache.paimon.index;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.IntIterator;
import org.apache.paimon.utils.PathFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;

/** Hash index file contains ints. */
public class HashIndexFile {

    public static final String HASH_INDEX = "HASH";

    private final FileIO fileIO;
    private final PathFactory pathFactory;

    public HashIndexFile(FileIO fileIO, PathFactory pathFactory) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
    }

    public long fileSize(String fileName) {
        try {
            return fileIO.getFileSize(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public IntIterator read(String fileName) throws IOException {
        Path path = pathFactory.toPath(fileName);
        BufferedInputStream in = new BufferedInputStream(fileIO.newInputStream(path));
        return new IntIterator() {

            @Override
            public int next() throws IOException {
                return readInt(in);
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        };
    }

    public String write(IntIterator input) throws IOException {
        Path path = pathFactory.newPath();
        try (BufferedOutputStream out =
                        new BufferedOutputStream(fileIO.newOutputStream(path, false));
                IntIterator iterator = input) {
            while (true) {
                try {
                    writeInt(out, iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        }
        return path.getName();
    }

    private int readInt(BufferedInputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    private void writeInt(BufferedOutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }
}
