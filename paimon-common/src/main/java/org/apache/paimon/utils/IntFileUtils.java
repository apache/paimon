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

package org.apache.paimon.utils;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.EOFException;
import java.io.IOException;

/** File to store ints. */
public class IntFileUtils {

    public static IntIterator readInts(FileIO fileIO, Path path) throws IOException {
        FastBufferedInputStream in = new FastBufferedInputStream(fileIO.newInputStream(path));
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

    public static void writeInts(FileIO fileIO, Path path, IntIterator input) throws IOException {
        try (FastBufferedOutputStream out =
                        new FastBufferedOutputStream(fileIO.newOutputStream(path, false));
                IntIterator iterator = input) {
            while (true) {
                try {
                    writeInt(out, iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        }
    }

    private static int readInt(FastBufferedInputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    private static void writeInt(FastBufferedOutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }
}
