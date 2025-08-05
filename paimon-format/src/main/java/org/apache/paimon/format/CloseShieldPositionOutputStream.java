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

package org.apache.paimon.format;

import org.apache.paimon.fs.PositionOutputStream;

import java.io.IOException;

/**
 * A proxy position output stream that prevents the underlying output stream from being closed or
 * flushed when already closed.
 */
public class CloseShieldPositionOutputStream extends PositionOutputStream {
    private final PositionOutputStream out;

    public CloseShieldPositionOutputStream(PositionOutputStream out) {
        this.out = out;
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        try {
            out.flush();
        } catch (IOException e) {
            // If the underlying stream is already closed, ignore the exception
            if (e.getMessage() != null && e.getMessage().contains("Already closed")) {
                // Silently ignore already closed exception
                return;
            }
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        // Do not actually close the internal stream.
    }
}
