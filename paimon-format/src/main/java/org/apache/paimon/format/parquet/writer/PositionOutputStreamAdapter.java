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

package org.apache.paimon.format.parquet.writer;

import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.util.Objects;

/** An adapter to turn Paimon's {@link PositionOutputStream} into a {@link PositionOutputStream}. */
class PositionOutputStreamAdapter extends PositionOutputStream {

    /** The Paimon stream written to. */
    private final org.apache.paimon.fs.PositionOutputStream out;

    /**
     * Create a new PositionOutputStreamAdapter.
     *
     * @param out The Paimon stream written to.
     */
    PositionOutputStreamAdapter(org.apache.paimon.fs.PositionOutputStream out) {
        this.out = Objects.requireNonNull(out, "out should not be null");
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
    public void write(byte[] buffer, int off, int len) throws IOException {
        out.write(buffer, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() {
        // we do not actually close the internal stream here, to prevent that the finishing
        // of the Parquet Writer closes the target output stream
    }
}
