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

package org.apache.flink.table.store.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.table.store.memory.MemorySegment;

import java.io.DataInput;
import java.io.IOException;

/**
 * This interface defines a view over some memory that can be used to sequentially read the contents
 * of the memory. The view is typically backed by one or more {@link MemorySegment}.
 */
@Public
public interface DataInputView extends DataInput {

    /**
     * Skips {@code numBytes} bytes of memory. In contrast to the {@link #skipBytes(int)} method,
     * this method always skips the desired number of bytes or throws an {@link
     * java.io.EOFException}.
     *
     * @param numBytes The number of bytes to skip.
     * @throws IOException Thrown, if any I/O related problem occurred such that the input could not
     *     be advanced to the desired position.
     */
    void skipBytesToRead(int numBytes) throws IOException;

    /**
     * Reads up to {@code len} bytes of memory and stores it into {@code b} starting at offset
     * {@code off}. It returns the number of read bytes or -1 if there is no more data left.
     *
     * @param b byte array to store the data to
     * @param off offset into byte array
     * @param len byte length to read
     * @return the number of actually read bytes of -1 if there is no more data left
     * @throws IOException
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Tries to fill the given byte array {@code b}. Returns the actually number of read bytes or -1
     * if there is no more data.
     *
     * @param b byte array to store the data to
     * @return the number of read bytes or -1 if there is no more data left
     * @throws IOException
     */
    int read(byte[] b) throws IOException;

    /**
     * TODO should be removed after table store implements serializer.
     *
     * @param inputView the input view
     * @return the delegated input view
     */
    static org.apache.flink.core.memory.DataInputView convertStoreToFlink(DataInputView inputView) {
        return new org.apache.flink.core.memory.DataInputView() {
            @Override
            public void skipBytesToRead(int numBytes) throws IOException {
                inputView.skipBytesToRead(numBytes);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return inputView.read(b, off, len);
            }

            @Override
            public int read(byte[] b) throws IOException {
                return inputView.read(b);
            }

            @Override
            public void readFully(byte[] b) throws IOException {
                inputView.readFully(b);
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {
                inputView.readFully(b, off, len);
            }

            @Override
            public int skipBytes(int n) throws IOException {
                return inputView.skipBytes(n);
            }

            @Override
            public boolean readBoolean() throws IOException {
                return inputView.readBoolean();
            }

            @Override
            public byte readByte() throws IOException {
                return inputView.readByte();
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return inputView.readUnsignedByte();
            }

            @Override
            public short readShort() throws IOException {
                return inputView.readShort();
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return inputView.readUnsignedShort();
            }

            @Override
            public char readChar() throws IOException {
                return inputView.readChar();
            }

            @Override
            public int readInt() throws IOException {
                return inputView.readInt();
            }

            @Override
            public long readLong() throws IOException {
                return inputView.readLong();
            }

            @Override
            public float readFloat() throws IOException {
                return inputView.readFloat();
            }

            @Override
            public double readDouble() throws IOException {
                return inputView.readDouble();
            }

            @Override
            public String readLine() throws IOException {
                return inputView.readLine();
            }

            @Override
            public String readUTF() throws IOException {
                return inputView.readUTF();
            }
        };
    }

    /** TODO should be removed after serializer. */
    static DataInputView convertFlinkToStore(org.apache.flink.core.memory.DataInputView inputView) {
        return new DataInputView() {
            @Override
            public void skipBytesToRead(int numBytes) throws IOException {
                inputView.skipBytesToRead(numBytes);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return inputView.read(b, off, len);
            }

            @Override
            public int read(byte[] b) throws IOException {
                return inputView.read(b);
            }

            @Override
            public void readFully(byte[] b) throws IOException {
                inputView.readFully(b);
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {
                inputView.readFully(b, off, len);
            }

            @Override
            public int skipBytes(int n) throws IOException {
                return inputView.skipBytes(n);
            }

            @Override
            public boolean readBoolean() throws IOException {
                return inputView.readBoolean();
            }

            @Override
            public byte readByte() throws IOException {
                return inputView.readByte();
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return inputView.readUnsignedByte();
            }

            @Override
            public short readShort() throws IOException {
                return inputView.readShort();
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return inputView.readUnsignedShort();
            }

            @Override
            public char readChar() throws IOException {
                return inputView.readChar();
            }

            @Override
            public int readInt() throws IOException {
                return inputView.readInt();
            }

            @Override
            public long readLong() throws IOException {
                return inputView.readLong();
            }

            @Override
            public float readFloat() throws IOException {
                return inputView.readFloat();
            }

            @Override
            public double readDouble() throws IOException {
                return inputView.readDouble();
            }

            @Override
            public String readLine() throws IOException {
                return inputView.readLine();
            }

            @Override
            public String readUTF() throws IOException {
                return inputView.readUTF();
            }
        };
    }
}
