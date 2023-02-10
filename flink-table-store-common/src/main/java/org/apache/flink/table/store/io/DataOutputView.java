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

import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface defines a view over some memory that can be used to sequentially write contents to
 * the memory. The view is typically backed by one or more {@link MemorySegment}.
 */
@Public
public interface DataOutputView extends DataOutput {

    /**
     * Skips {@code numBytes} bytes memory. If some program reads the memory that was skipped over,
     * the results are undefined.
     *
     * @param numBytes The number of bytes to skip.
     * @throws IOException Thrown, if any I/O related problem occurred such that the view could not
     *     be advanced to the desired position.
     */
    void skipBytesToWrite(int numBytes) throws IOException;

    /**
     * Copies {@code numBytes} bytes from the source to this view.
     *
     * @param source The source to copy the bytes from.
     * @param numBytes The number of bytes to copy.
     * @throws IOException Thrown, if any I/O related problem occurred, such that either the input
     *     view could not be read, or the output could not be written.
     */
    void write(DataInputView source, int numBytes) throws IOException;

    /**
     * TODO should be removed after table store implements serializer.
     *
     * @param outputView the output view
     * @return the delegated output view
     */
    static DataOutputView convertFlinkToStore(
            org.apache.flink.core.memory.DataOutputView outputView) {
        return new DataOutputView() {
            @Override
            public void skipBytesToWrite(int i) throws IOException {
                outputView.skipBytesToWrite(i);
            }

            @Override
            public void write(DataInputView dataInputView, int i) throws IOException {
                outputView.write(DataInputView.convertStoreToFlink(dataInputView), i);
            }

            @Override
            public void write(int b) throws IOException {
                outputView.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                outputView.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputView.write(b, off, len);
            }

            @Override
            public void writeBoolean(boolean v) throws IOException {
                outputView.writeBoolean(v);
            }

            @Override
            public void writeByte(int v) throws IOException {
                outputView.writeByte(v);
            }

            @Override
            public void writeShort(int v) throws IOException {
                outputView.writeShort(v);
            }

            @Override
            public void writeChar(int v) throws IOException {
                outputView.writeChar(v);
            }

            @Override
            public void writeInt(int v) throws IOException {
                outputView.writeInt(v);
            }

            @Override
            public void writeLong(long v) throws IOException {
                outputView.writeLong(v);
            }

            @Override
            public void writeFloat(float v) throws IOException {
                outputView.writeFloat(v);
            }

            @Override
            public void writeDouble(double v) throws IOException {
                outputView.writeDouble(v);
            }

            @Override
            public void writeBytes(String s) throws IOException {
                outputView.writeBytes(s);
            }

            @Override
            public void writeChars(String s) throws IOException {
                outputView.writeChars(s);
            }

            @Override
            public void writeUTF(String s) throws IOException {
                outputView.writeUTF(s);
            }
        };
    }

    /** TODO should be removed after table store implements serializer. */
    static org.apache.flink.core.memory.DataOutputView convertStoreToFlink(
            DataOutputView outputView) {
        return new org.apache.flink.core.memory.DataOutputView() {
            @Override
            public void skipBytesToWrite(int i) throws IOException {
                outputView.skipBytesToWrite(i);
            }

            @Override
            public void write(org.apache.flink.core.memory.DataInputView dataInputView, int i)
                    throws IOException {
                outputView.write(DataInputView.convertFlinkToStore(dataInputView), i);
            }

            @Override
            public void write(int b) throws IOException {
                outputView.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                outputView.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                outputView.write(b, off, len);
            }

            @Override
            public void writeBoolean(boolean v) throws IOException {
                outputView.writeBoolean(v);
            }

            @Override
            public void writeByte(int v) throws IOException {
                outputView.writeByte(v);
            }

            @Override
            public void writeShort(int v) throws IOException {
                outputView.writeShort(v);
            }

            @Override
            public void writeChar(int v) throws IOException {
                outputView.writeChar(v);
            }

            @Override
            public void writeInt(int v) throws IOException {
                outputView.writeInt(v);
            }

            @Override
            public void writeLong(long v) throws IOException {
                outputView.writeLong(v);
            }

            @Override
            public void writeFloat(float v) throws IOException {
                outputView.writeFloat(v);
            }

            @Override
            public void writeDouble(double v) throws IOException {
                outputView.writeDouble(v);
            }

            @Override
            public void writeBytes(String s) throws IOException {
                outputView.writeBytes(s);
            }

            @Override
            public void writeChars(String s) throws IOException {
                outputView.writeChars(s);
            }

            @Override
            public void writeUTF(String s) throws IOException {
                outputView.writeUTF(s);
            }
        };
    }
}
